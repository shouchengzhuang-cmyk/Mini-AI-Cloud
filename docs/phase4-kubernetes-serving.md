# Phase IV-A：Kubernetes 原生模型服务

Phase IV-A 给现有 Model Service 增加一个 Kubernetes backend。它只负责 Fake inference Pod，用来在普通开发机上验证真实 Kubernetes workload、readiness、Gateway、扩缩容和恢复。Kubernetes vLLM、跨节点 tensor parallel、Operator、KServe、Service Mesh 和公有云资源管理不在本阶段范围内。

## 请求如何走到 Pod

```text
RegisteredModel / ModelService
              |
              v
      ServiceReconciler
      创建 PENDING Replica
              |
              v
KubernetesReplicaRuntimeController
      execution / lease fencing
              |
              v
 KubernetesServingRuntimeAdapter
        Pod + ClusterIP Service
              |
              v
 readinessProbe GET /health
              |
              v
 Replica RUNNING + HEALTHY
              |
              v
 OpenAI-compatible Gateway
```

PostgreSQL 仍保存 desired state、Replica 生命周期、`execution_id`、generation、lease、endpoint 和请求计数。Kubernetes 是 workload 的实际运行环境，不取代数据库里的所有权判断。Service Reconciler 只创建或收缩 Replica 记录，Kubernetes controller claim `runtime=fake,runtime_type=kubernetes` 的 pending Replica，再创建资源并推进状态。

每个 execution 对应一个 Pod 和一个 ClusterIP Service。Service selector 包含 Replica、generation、execution 和 controller worker identity，旧 execution 不会被新流量误选。Gateway 使用集群内 DNS：

```text
http://<service-name>.<namespace>.svc.cluster.local:8000
```

API 和 Gateway 部署在同一 Kind 集群时，应把对应的 `*.svc.cluster.local` 后缀加入 `SERVICE_ENDPOINT_HOST_ALLOWLIST`。Gateway 仍走原有 endpoint 选择、round-robin、active request 和 SSE 转发代码，没有测试专用旁路。

## 资源身份和 fencing

资源名称经过 DNS-1123 清洗，并把完整身份的哈希放进有界名称。Pod 和 Service 带以下稳定标签：

```text
mini-ai-cloud/managed
mini-ai-cloud/resource-kind
mini-ai-cloud/service-id
mini-ai-cloud/replica-id
mini-ai-cloud/project-id
mini-ai-cloud/generation
mini-ai-cloud/execution-id
mini-ai-cloud/cluster-id
mini-ai-cloud/worker-id
mini-ai-cloud/worker-session-id
mini-ai-cloud/runtime
mini-ai-cloud/spec-hash
```

create 返回 409 时，runtime 会读取同名资源并逐项核对身份、spec hash 和安全边界。任一 fence 不一致都会拒绝 adopt。删除前也会核对标签，并使用 Pod UID precondition，旧 handle 不能删除同名的新 Pod。

Controller 使用稳定 virtual Worker ID，每次进程启动生成新的 worker session。数据库 session 已被新进程替换后，旧 controller 的续租和终态写入会失败。旧进程遇到这种情况只丢弃本地 handle，不删除 Pod，因为新进程可能已经接管同一个 execution。

## Fake inference 和 readiness

Inference Pod 运行仓库里的 `scripts.fake_inference`，提供：

```http
GET  /health
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
```

`KUBERNETES_SERVING_FAKE_STARTUP_DELAY` 控制加载时间。延迟结束前，`/health` 返回 503，Pod 可以处于 `Running`，但不会 Ready。Controller 先把 Replica 记为 `loading`，只有 Kubernetes Ready condition 成立后才写入 `running + healthy`。`KUBERNETES_SERVING_FAKE_CHUNK_DELAY` 控制每个 SSE chunk 的间隔，便于在 streaming request 未结束时验证 draining。

Fake Kubernetes serving 需要同时打开总开关和测试开关，并且只允许 `development` 或 `test`：

```dotenv
APP_ENV=test
KUBERNETES_SERVING_ENABLED=true
KUBERNETES_SERVING_FAKE_ENABLED=true
KUBERNETES_SERVING_IMAGE=mini-ai-cloud:kind-serving-v4a
```

生产配置默认关闭。即使误设开关，Settings 和 API admission 也会拒绝 production Fake fallback。镜像仍经过项目的 Image Policy，Kind 脚本只放宽到 E2E 使用的固定 repository 和 tag。

## Pod 安全边界

Serving Pod 不挂载 Docker socket、kubeconfig 或 ServiceAccount token，也不使用 `hostPath`。Pod spec 固定以下约束：

- `runAsNonRoot=true`，UID/GID 为镜像内的 10001。
- `allowPrivilegeEscalation=false`、`privileged=false`。
- `readOnlyRootFilesystem=true`，只给 `/tmp` 挂载有 64 MiB 上限的 memory `emptyDir`。
- 丢弃全部 Linux capabilities，seccomp 使用 `RuntimeDefault`。
- `hostNetwork`、`hostPID`、`hostIPC` 均关闭。
- CPU 和内存 requests 等于 limits，避免控制面声明与 Pod 实际 reservation 分离。
- `restartPolicy=Never`，由平台创建带新 execution 的 replacement。
- `terminationGracePeriodSeconds` 由平台配置。

API/controller Pod 使用 namespace 内最小 RBAC，只能读取、创建、更新和删除本阶段管理的 Pod 与 Service。Kubernetes RBAC 不能按 label 限制，因此生产部署仍应给 controller 独立 namespace 和 ServiceAccount。

## 生命周期、扩缩容和 drain

正常路径保持 Phase III 的状态语义：

```text
pending -> starting -> loading -> running + healthy
        -> draining -> stopping -> stopped
```

scale 2 到 4 时，Service Reconciler 新建两个 pending Replica，controller 给每个 Replica 分配新 execution 并等待 readiness。scale 4 到 1 时，三个 Replica 先进入 `draining`。Gateway 只选择 `running + healthy`，所以它们不再接收新请求；数据库里的 `active_requests` 让 controller 等现有 HTTP 或 SSE 请求结束。请求释放后发送 graceful delete，超过 drain deadline 才 force cleanup。

Pod `Running` 但尚未 Ready 不接流量。已 Ready 的 Pod 失去 readiness 后会被标为 unhealthy，Gateway 同样不会选择它。

## 故障和重启恢复

Controller 将常见 Pod 故障映射到有界错误码：

| Kubernetes 状态 | Replica 结果 |
| --- | --- |
| `ErrImagePull`、`ImagePullBackOff` | `IMAGE_PULL_FAILED` |
| 容器创建或启动失败 | `CONTAINER_START_FAILED` |
| `OOMKilled` | `OOM_KILLED` |
| startup deadline 到期 | `MODEL_LOAD_TIMEOUT` |
| Pod 丢失或被外部删除 | `LOST / WORKER_LOST` |

错误消息会截断，Prometheus reason 只使用固定枚举。失败 Replica 进入终态后，普通 Service Reconciler 创建 replacement。服务的 `scheduling_details` 保存 `retry_not_before` 和有上限的指数 backoff，坏镜像不会每个控制周期创建新 Pod。

Controller 启动时先注册新 worker session，再执行恢复，然后才进入普通控制循环：

1. 按 managed、cluster 和 worker labels 列出资源。
2. 用 service、replica、generation 和 execution labels 匹配数据库里的 active Replica。
3. adopt 合法资源并立即续租，保留原 Pod 和 endpoint。
4. 清理数据库中没有合法 owner 的 orphan 资源。
5. 数据库记录存在但 Pod 已丢失时，写入明确终态，交给 reconciler 补齐。

应用 shutdown 只关闭 Kubernetes client，不删除健康 Pod。Controller rollout restart 后，Pod UID 和 execution 应保持不变，也不应多出一组重复 Replica。

## 配置

| 环境变量 | 默认值 | 用途 |
| --- | ---: | --- |
| `KUBERNETES_SERVING_ENABLED` | `false` | Kubernetes serving 总开关 |
| `KUBERNETES_SERVING_FAKE_ENABLED` | `false` | development/test Fake Pod 二次开关 |
| `KUBERNETES_SERVING_NAMESPACE` | `mini-ai-cloud-serving` | Pod、Service 和 controller RBAC 所在 namespace |
| `KUBERNETES_SERVING_CLUSTER_ID` | `mini-ai-cloud-local` | managed resource 和 virtual Worker 的集群 fence |
| `KUBERNETES_SERVING_IMAGE` | 空 | Fake inference 默认镜像，仍受项目 Image Policy 约束 |
| `KUBERNETES_SERVING_STARTUP_TIMEOUT` | `120` | 等待 readiness 的最长秒数 |
| `KUBERNETES_SERVING_DRAIN_TIMEOUT` | `30` | drain 和 graceful stop 的最长秒数 |
| `KUBERNETES_SERVING_POLL_INTERVAL` | `1` | controller 观察周期 |
| `KUBERNETES_SERVING_PROBE_TIMEOUT` | `3` | readiness probe timeout |
| `KUBERNETES_SERVING_LEASE_SECONDS` | `180` | Replica ownership lease |
| `KUBERNETES_SERVING_FAILURE_BACKOFF` | `5` | 首次 replacement backoff |
| `KUBERNETES_SERVING_TERMINATION_GRACE_SECONDS` | `30` | Pod SIGTERM grace period |
| `KUBERNETES_SERVING_FAKE_STARTUP_DELAY` | `0` | Fake model loading delay |
| `KUBERNETES_SERVING_FAKE_CHUNK_DELAY` | `0.02` | Fake SSE chunk delay |

集群外开发可继续复用 `KUBERNETES_KUBECONFIG`，集群内 controller 设置 `KUBERNETES_IN_CLUSTER=true`。不要把个人 kubeconfig 放进镜像或 inference Pod。

## Kind E2E

脚本使用专用 cluster `mini-ai-cloud-serving-v4a` 和仓库内 kubeconfig `build/kind-serving/kubeconfig`，不会切换默认 kubectl context。宿主端口 `18080` 映射到 API NodePort `30080`。

```bash
make kind-serving-up
make test-kind-serving
make kind-serving-down
```

`kind-serving-up` 检查 Docker、Kind 和 kubectl，构建固定 tag 镜像、load 到 Kind、部署 PostgreSQL/Redis、执行 migration，再启动 API/controller。缺少前置命令时会输出 `NOT RUN` 并返回非零，不会静默 skip 或安装全局软件。

E2E 通过真实 API 创建 Project、API Key、Image Policy、Registry Model 和 Kubernetes-backed Service。断言范围包括 2 个 ready Replica、`/v1/models`、non-streaming、SSE、round-robin、手工删 Pod 后 replacement、2 到 4、带 active stream 的 4 到 1、controller rollout restart 后 adopt，以及坏镜像 loading failure 与 backoff。脚本不会把 API Key 写入日志。

本次机器是否实际完成这些 Kind 断言，以 [Phase IV-A 验证报告](verification-report-phase4a-2026-08-24.md) 为准。manifest/unit test 通过只能证明资源生成和状态映射，不能写成真实 Kubernetes E2E。

## 当前限制

- 只支持单 Kubernetes serving cluster，ModelService 还没有目标 cluster 字段。
- Kubernetes backend 只运行 Fake inference，真实 vLLM/GPU 留到后续阶段。
- 没有跨节点 tensor parallel、MIG、KServe、CRD/Operator、canary 或 service mesh。
- ClusterIP DNS 要求 Gateway 位于集群内，集群外 Gateway 需要另行设计受控 endpoint。
- PostgreSQL、Redis 和 API 的 Kind manifest 面向本地验收，不是 HA 或公网生产部署方案。
