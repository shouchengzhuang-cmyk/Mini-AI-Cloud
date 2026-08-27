# 架构与一致性边界

本文描述代码中的实际边界，而不是目标架构愿望。若本文与运行中的 schema、配置或 OpenAPI 冲突，以现场实测为准。

## 组件拓扑

```text
Client / CLI
  |
  | REST, SSE, WebSocket, OpenAI-compatible HTTP
  v
FastAPI API
  |-- authentication, RBAC, project isolation, rate/size admission
  |-- task/service/artifact/model/secret APIs
  |-- gateway and admin diagnostics
  |
  +-----------------------------+
  |                             |
  v                             v
PostgreSQL                  Redis
source of truth             notification/cache/rate limit
  |                             |
  +-------------+---------------+
                |
                v
      Control Plane / Global Scheduler
       | outbox | reaper | reconciliation
       | health | autoscaler | cleanup boundary
                |
      +---------+-------------------+
      |                    |        |
      v                    v        v
Docker Worker       Kubernetes   Kubernetes
                    Batch Worker  Serving Controller
      |                    |        |
task container         Pod/Job   Pod + static headless DNS

Artifact metadata -> PostgreSQL
Artifact bytes    -> Local volume or S3-compatible store
```

API 可以与后台 controller 同进程运行，也可以通过 `CONTROL_PLANE_ENABLED=false` 把 API 与控制循环拆开。多个 scheduler 依靠 PostgreSQL 行锁和 `SKIP LOCKED` 竞争任务；Redis 锁不是调度正确性的前提。

## 状态真相

| 数据 | 权威来源 | 非权威加速层 |
| --- | --- | --- |
| Task 状态、当前执行权、lease | PostgreSQL | Redis ready 通知 |
| 资源 reservation、accelerator allocation intent/evidence 与 exact device ownership | PostgreSQL | Worker heartbeat inventory |
| Project quota 与并发计数 | PostgreSQL 锁内更新 | 无 |
| Usage/成本 | immutable usage ledger | 聚合查询 |
| Service desired/actual state、replica lease | PostgreSQL | controller 内存节拍 |
| Artifact metadata、project quota | PostgreSQL | 无 |
| Artifact bytes | Local/S3 object store | presigned URL |
| 持久日志 | PostgreSQL | Redis wake-up stream |
| Domain/Audit events | PostgreSQL outbox/audit table | WebSocket/Redis delivery |

Redis 数据丢失会降低实时性，但 Worker 有 PostgreSQL fallback，任务状态不会因此凭空消失。PostgreSQL 不可用时系统不能安全接受写入或提交终态，因此 readiness 应为 degraded。

## Batch Job 生命周期

```text
request
  -> admission (auth/RBAC, schema, quota, image policy, runtime/resource check)
  -> task + quota reservation + outbox in one DB transaction
  -> queued
  -> global placement under row locks
  -> execution_id + worker/GPU reservation + lease
  -> runtime prepare/start/logs/wait
  -> fenced terminal write
  -> resource/quota release + usage settlement
```

Phase II 状态机包含 `pending`、`queued`、`scheduling`、`assigned`、`preparing`、`pulling`、`starting`、`running`、`preempting`、`preempted`、`stopping`、`succeeded`、`failed`、`cancelled`、`timed_out`、`retrying`。所有状态修改必须走受测的合法转移，不允许路由直接任意改字符串。

### Fencing

每次合法分配都生成新的 `execution_id`：

```text
Worker A owns execution A1
A loses lease
Reaper revokes A1 and releases/requeues according to policy
Worker B owns execution B1
A reports success for A1 -> rejected
B reports for B1       -> accepted
```

这提供“控制面终态写入 fencing”，不是业务 exactly-once。A 在外部系统已触发的邮件、支付或数据库写入无法撤销，用户 workload 仍需自己的幂等键。

### 两阶段抢占

抢占不会先释放 GPU 再等待旧容器停下：scheduler 先持久化 preemption plan 并把 victim 置为 `preempting`，旧 execution 收到 stop 后通过 fenced 结果释放 reservation，随后高优先级任务才能获取同一设备。这样避免同一 GPU 在物理停止窗口中被双重分配。

## 调度模型

调度分层考虑：

1. Project quota 与 dominant-resource fair share。
2. Task priority、FIFO 与 age bonus。
3. Worker online/draining、runtime type、label 与 taint/toleration。
4. CPU/RAM、GPU count、GPU free memory 和 GPU model。
5. `binpack` 或 `spread` 打分。
6. 可选的显式 preemption；只有 `preemptible=true` 的低优先级任务可成为 victim。

调度成功时，TaskExecution、ResourceReservation、ReservationGPUDevice 与 Worker accounting 在同一事务写入。终态、取消、timeout、lease recovery 和 stale execution 都必须走幂等释放路径。

Worker re-register 不得把旧 execution 已持有的 reservation 清零；它只更新声明的总资源和 inventory，过量占用时进入 draining，由 lease/终态路径负责释放。

### Accelerator allocation authority

PostgreSQL 同时保存 accelerator 的请求快照与观测证据，但设备绑定方式由 `allocation_authority` 区分：

- `control_plane_exact_device`：控制面在调度事务内写入 `ReservationGPUDevice` 链接，并立即保存与链接一致的 device ID 和 vendor 观测值。
- `kubernetes_device_plugin`：控制面只保存 vendor/kind/profile 请求，不创建 exact-device 链接；只有 Pod 或 device plugin 报告后才能原子写入观测到的 device IDs。

请求快照与观测证据同步保存在 reservation 和 execution 上，便于终态后追溯。终态释放会清除 active reservation/device ownership，但不删除 execution 上的历史快照。

## Runtime abstraction

Worker 面向 `ComputeRuntime`，不在 executor 中分支 Docker/Kubernetes 实现细节。运行时生命周期为：

```text
prepare -> start -> logs + wait -> stop -> cleanup
```

- Docker：创建受限容器，使用具体 NVIDIA device request，不使用 `--gpus all`。
- Kubernetes：创建带 task/project/execution labels 的对象，支持资源 request/limit、GPU、日志、停止和 reconciliation；Pod/Container 固定 UID/GID 65532、`runAsNonRoot`、`RuntimeDefault` seccomp、只读 rootfs 与 drop ALL。
- Fake：只用于 development/test，使无 GPU、无 vLLM 环境仍可验证完整控制面。
- Docker vLLM controller：默认关闭，只能在具备 Docker socket 与真实 NVIDIA inventory 的专用 serving node 显式启用。它以独立 draining Worker 身份领取 replica intent，按 generation/execution/session fencing 管理容器，并为每个 Project/Service 使用隔离的缓存卷；本机无 GPU 时只验证 lifecycle/spec，不把它写成真实推理实测。
- Kubernetes serving：采用独立 long-running runtime boundary，不复用 batch `wait/logs/terminal` 语义。它为 Fake Replica 只创建 Pod，通过预置的 headless Service 获得 Pod 专属 DNS，等 Kubernetes Ready condition 成立后才开放 Gateway 流量；controller 没有 Service 写权限。应用 shutdown 只关闭 client，启动恢复会 adopt 仍由数据库 execution 持有的 Pod。

`FAKE_GPU_*` 在 production 配置下会被拒绝。Kubernetes client 的代码路径与单测存在，但没有真实 cluster 的机器只能把它报告为“implemented, environment-limited”，不能写成已完成 K8s E2E。

## Model Service 控制循环

ModelService 保存 desired replicas，ServiceReplica 保存 generation、execution fencing、lease、endpoint 与 health：

```text
desired replicas
      |
      v
reconciler compares desired vs actual
  | create missing replicas
  | stop excess replicas
  | recover expired leases
  | replace unhealthy replicas
      v
gateway selects one healthy replica (round robin)
```

Gateway 只代理请求，不实现推理。它在转发前验证 API Key、Project ownership 与 readiness，过滤 hop-by-hop/敏感 header，并对 endpoint 做 SSRF 边界检查。非 SSE 响应以 64 KiB 分块读取，并受 `SERVICE_PROXY_MAX_RESPONSE_BYTES`（默认 16 MiB）硬上限保护；SSE 保持真正流式。Fake inference server 用来验证控制面；真实 vLLM 需要 NVIDIA GPU、合适模型和独立容量验证。

Phase IV-A controller claim `runtime=fake,runtime_type=kubernetes` 的 Replica。每个 execution 使用带完整 fencing labels 的 Pod 和精确 selector Service，Gateway 在集群内访问 `.svc.cluster.local` endpoint。startup recovery 在普通 controller loop 创建前完成，避免 rollout 时重复创建健康 Replica。扩缩容继续使用数据库 desired state 和 `active_requests`：进入 draining 的 Replica 不再被 Gateway 选择，现有 HTTP/SSE 请求释放或 deadline 到期后才删除 Pod。完整资源和 Kind 边界见 [Phase IV-A Kubernetes 原生模型服务](phase4-kubernetes-serving.md)。

## 多租户与权限

API Key principal 固定绑定一个 Project、User 与 Membership role。中央 RBAC permission map 负责授权：

| Role | 典型权限 |
| --- | --- |
| viewer | 读 task/log/usage/cost/model |
| member | viewer + submit/cancel own/use secret |
| admin | member + service/model/secret/API key/quota/image policy/audit/worker 管理或读取 |
| owner | Project 内全部权限与 membership 管理 |

Project-scoped 查询必须带 `project_id` 条件。跨项目对象通常返回 404，而非 403，以免 UUID 被用作存在性探针。普通 Project 用户不应看到其他租户的 worker rejection 明细、endpoint 或 audit 数据。

匿名 legacy principal 只用于 Phase I 兼容，权限和 workload 类型被刻意限制。共享或公网环境应设置 `LEGACY_ANONYMOUS_ENABLED=false`。

## Secret 与 Artifact

Secret 使用 AES-256-GCM：密文、nonce 和 key id 存数据库，associated data 绑定 project、secret id 与 version。`SECRET_MASTER_KEY` 是 key ring，第一项为当前写入 key；旧 key 在轮换完成前必须保留以便解密历史版本。GET 只返回 metadata。

Task 只引用 secret id/version/env name。Worker 在执行边界解密并临时注入，日志对原文做 best-effort replacement。直接 echo 的原文会被替换；base64、压缩、分片或其他变换不在可证明保护范围。

Artifact 创建先在 PostgreSQL 预留 project bytes，再上传到 staging key，finalize 时检查 size 和 SHA-256 后原子晋升。客户端只看到授权 API URL 或短期 presigned URL，不看到 object key。对象 key 与下载文件名都拒绝路径穿越。

Task schema 可声明 `inputs` 与输出 `artifacts`。`ArtifactWorkspaceManager` 按 project/worker/`execution_id` 读取 binding，在私有目录流式物化 input 并复验 size/SHA-256；TaskExecutor 将生成的单文件 mounts 传给 runtime，只在 exit 0 时、提交 terminal transition 前发布 outputs，并在 `finally` 清理 workspace。必需 output 发布失败会使任务失败，不能先写 succeeded 再丢产物。

Docker runtime 有两种文件交付方式。默认 Compose 中，Worker 在随 Compose project 派生的 `artifact-workspace-data` 卷内准备 workspace；通过 socket 创建的 sibling task container 使用同一 Docker volume，并以 `VolumeOptions.Subpath` 逐个挂载已声明文件，从而不依赖 Worker 容器路径在 daemon 宿主命名空间中可见，也不与其他 Compose project 共享 workspace。裸机 Worker 与 Docker daemon 共享宿主文件系统时，不配置 workspace volume，runtime 继续使用受控的单文件 bind。两种方式都是 input `ro`、output `rw`，都不把整个 artifact root 暴露给 workload。

Kubernetes runtime 把 Pod 固定到 Worker 声明的 node，并为每个文件生成 `hostPath(type=File)`；这要求 Worker workspace 与目标 node 共享同一绝对路径。当前证据包括 executor/workspace 自动化测试和真实 Docker named-volume Subpath input→container→output E2E；Kubernetes 仍只有 Pod/NetworkPolicy spec 测试，尚未在 Kind 验证真实文件可见性。生产多节点更适合受控 object-store download/upload、PVC 或 CSI，而不是跨节点假设 hostPath 可见。

## DAG

Task 可通过 `depends_on` 表达同 Project 依赖；依赖未全部成功时保持 `pending`，成功后由 reaper/控制面晋升 `queued`。失败策略可选择 block 或 cancel。Job Group 保存显式边，写入前执行 cycle detection；scheduler claim 和最终 placement 仍会重新检查依赖，防止 Redis 重复通知或竞争绕过 DAG gate。

## 关键不变量

- 一个 Task 同一时刻至多一个当前 execution。
- 旧 `execution_id` 不能续租、追加有效结果或覆盖新执行。
- 一个 GPU device 同一时刻至多一个 active reservation。
- exact-device allocation 必须有与请求数量、vendor 一致的具体设备链接和观测 ID。
- Kubernetes device-plugin allocation 在观测前不得伪造 device ID，且不得持有 exact-device 链接。
- terminal Task 不应保留 active reservation；释放和 usage settlement 幂等。
- quota state 不为负，且并发创建不能越过上限。
- API Key/Secret 明文不得出现在数据库、响应列表、结构化日志或异常文本。
- Project A 的 credential 不能读写 Project B 的资源。
- Service actual replicas 最终向 desired replicas 收敛；过期 lease 的 replica 不继续被路由。
- Artifact bytes 只有 size/hash 校验通过后才进入 ready；被引用对象不能由 retention 误删。

对应验证入口见 [验证矩阵](verification-matrix.md)。

## 明确未承诺的边界

- 不承诺跨外部副作用 exactly-once。
- 不承诺 Docker socket Worker 对恶意基础设施代码的隔离。
- 不承诺仅靠 `network_enabled` 实现细粒度 egress policy。
- 不承诺预留的 `WORKER_AUTH_TOKEN` 已认证当前 DB/Redis 直连，更不等价于节点 mTLS identity。
- 不承诺 mock Pod spec 已证明 Kubernetes node 上能看到 Worker 的本地 Artifact workspace。
- 不承诺没有真实 GPU/cluster 时的 vLLM/Kubernetes 性能与兼容性。
- 不实现真实支付、镜像签名/漏洞扫描、HA PostgreSQL/Redis 或多区域灾备。
