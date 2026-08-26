# Phase III：AI Model Serving

Phase III 在 Phase II 的多租户控制面、Model Service、Fake inference、Gateway 和 GPU inventory 上增量补齐一条可运行的模型部署与在线推理链路。PostgreSQL 仍是 desired state、Replica actual state、请求计数和 usage 的状态真相；Gateway 不执行模型，只负责鉴权、解析 Service、选择健康 Replica 和转发响应。

## Phase II 审计与最小改造

| 能力 | Phase II 状态 | Phase III 处理 |
| --- | --- | --- |
| Model Registry | 已有基础 identity/source/revision/resource metadata | 增加 runtime、默认 GPU 数和类型化 runtime defaults；部署时固化快照 |
| Model Service / Replica | 已有 desired replicas、generation、replica lease 和 reconcile | 增加 loading/draining、runtime/spec 快照、ready/drain 时间、error code 和持久化 active requests |
| Fake inference | 已有真实子进程 HTTP server | 补齐 loading、稳定 Worker session、orphan recovery、SSE usage 和可控分块延迟 |
| Docker vLLM | 已有显式 opt-in controller 和精确 GPU ID | 增加安全参数 allowlist、model readiness、load timeout、TP gang placement、revision/dtype/memory 配置和 image digest |
| Gateway | 已有 OpenAI-compatible proxy 和 round-robin | 补齐真实流式转发、三类 timeout、取消清理、持久化计数、usage/TTFT/token/错误指标 |
| GPU scheduling | 已有通用任务 GPU reservation | 增加面向 serving 的单节点全量 gang placement 和 scheduling explain；不做跨节点 TP |
| Autoscaling / health | 已有 concurrency autoscaler 和阈值健康检查 | 复用现有实现；draining Replica 不再接收新请求，健康失败进入 replacement |
| Usage | 已有 Batch execution ledger | 增加 serving request ledger、后端报告 token、请求归因 GPU 秒和 Replica GPU 运行秒 |

没有为了状态名称重构原有 Service 聚合，也没有引入 Ray、Slurm、Kubernetes Operator 或新的消息系统。

## 部署流程与状态真相

```text
Registry model / direct spec
        |
        v
ModelService desired_replicas + immutable runtime snapshot
        |
        v
ServiceReconciler (service row lock / SKIP LOCKED)
        |
        v
ServiceReplica pending -> starting -> loading -> running
        |                                  |
        |                                  +-> health=healthy -> Gateway eligible
        +-> runtime launch/readiness failure -> failed/lost -> replacement
```

- `ModelService.desired_replicas` 是用户意图；Replica 行是实际状态。
- Reconciler 在数据库事务内锁定 Service，同一 `(service_id, generation, ordinal)` 还有唯一约束，因此多个 Reconciler 不会把 `desired=2` 持久化为 4 个当前 Replica。
- Runtime 领取 Replica 时写入新的 `execution_id` 和 Worker session。续租、Ready、终态和请求释放都带 generation/execution fencing。
- 容器或 Fake 进程启动只进入 `loading`；只有 readiness/health endpoint 成功后才进入 `running + healthy`。
- `MODEL_LOAD_TIMEOUT` 与启动失败分别持久化为 `MODEL_LOAD_TIMEOUT`、`MODEL_LOAD_FAILED`。

Service 保留 Phase II 的聚合状态：`pending/deploying/running/degraded/stopping/stopped/failed`。Replica 使用更精确的 actual state：

```text
pending -> starting -> loading -> running -> draining -> stopped
                      |          |
                      +-> failed +-> unhealthy -> draining/stopping -> replacement
```

## Registry 到 Service 的快照

Registry 记录 source、revision、runtime、默认 GPU 数、预计 GPU memory 和以下类型化默认值：

```text
gpu_model
tensor_parallel_size
dtype
gpu_memory_utilization
max_model_len
```

创建 Service 时可只提交 `registered_model_id`。API 只在当前 Project 内查询该 Registry 记录；不存在和跨 Project 都返回 `404 MODEL_NOT_FOUND`。解析后的 source、revision、runtime 和资源参数会写入 Service。之后删除 Registry 行只把 `registered_model_id` 置空，已部署 Service 的快照不变。

请求中显式提供的字段优先于 Registry 默认值，但解析完成后必须重新通过同一套 runtime/resource 校验。Registry JSON 不能注入任意 vLLM 参数。

## Serving Runtime

### FakeServingRuntime

Fake runtime 只在 `APP_ENV=development|test` 创建，production 不会静默启用。每个 Replica 是一个真实 Python HTTP 子进程，提供：

```http
GET  /health
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
```

Chat 和 text completions 均支持 non-streaming；chat 支持 SSE streaming。Controller 重启时会使用稳定 Worker identity 清理旧 Fake 进程，数据库中缺失的 runtime 会被标记 lost/stopped 后重新 reconcile。

### DockerVLLMRuntime

真实路径使用受控的 `VLLMLaunchRequest -> VLLMLaunchSpec`，而不是拼接用户 shell。允许的核心字段为：

```text
model / revision / dtype / tensor_parallel_size
gpu_memory_utilization / max_model_len
```

`dtype` 和 extra arguments 使用 allowlist；GPU 容器只看见 Scheduler 选中的 UUID，`tensor_parallel_size` 必须等于可见 GPU 数。容器启动后先探测 readiness，成功才发布 Gateway endpoint。最终容器 image digest 会写入 Replica。

`VLLM_IMAGE` 只是未显式提供 image 时的默认值，仍必须经过 Project image policy；它不会绕过 digest 或 allowlist。默认 Compose 不给 API 挂 Docker socket，且 `SERVICE_VLLM_DOCKER_ENABLED=false`。

## GPU 与 Tensor Parallel Placement

`gpu_count=4, tensor_parallel_size=4` 被视为单节点 gang allocation：同一 Worker 能一次提供全部 4 张合格 GPU 才成功，否则不占任何 GPU。筛选同时考虑 `gpu_model`、单卡可用 memory 和 Fake/real 标记。

调度失败会在 Service 上保存 bounded reason/details，例如：

```json
{
  "reason": "INSUFFICIENT_CONTIGUOUS_GPUS",
  "requested_gpu_count": 4,
  "largest_available_worker_gpu_count": 2,
  "requested_gpu_model": "A100",
  "required_gpu_memory_mb": 40960
}
```

Fake GPU 默认不会进入真实 vLLM placement。测试 Demo H 必须显式 `allow_fake=true`；这只证明 gang scheduling policy，不代表 NVIDIA/CUDA 已运行。

## Gateway、Streaming 与 Draining

统一入口为：

```http
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
```

请求链路：

```text
API Key -> Project -> private ModelService -> RUNNING+HEALTHY Replica
        -> persistent active_requests + round-robin cursor
        -> HTTP upstream -> buffered JSON or incremental SSE downstream
        -> fenced release + request usage settlement
```

- Project 条件参与 Service 查询；另一个 Project 的同名请求得到 404，且不会推进目标 Service 的 RR cursor。
- unhealthy、loading、draining Replica 不获得新请求。
- 选择 Replica 与 `active_requests += 1` 在数据库事务中完成；正常返回、超时、上游断开、客户端取消和下游 send 失败都会执行 fenced release。
- `x-mini-ai-replica-id` 便于本地验证 RR；客户端传入的同名 header 会被剥离，不能伪造给 upstream。
- SSE 边读 upstream 边发送 downstream，不把整个响应放进内存。响应头发出后若 overall timeout，只能终止流并记录错误，HTTP 状态无法再改成 503。
- connect、first-token、overall deadline 分别由 `SERVICE_PROXY_CONNECT_TIMEOUT`、`SERVICE_PROXY_FIRST_TOKEN_TIMEOUT`、`SERVICE_PROXY_TIMEOUT` 控制。

Scale down 时，`running -> draining` 会先从新流量中摘除。Runtime 等待持久化 `active_requests == 0`，或到达 `SERVICE_DRAIN_TIMEOUT` 后停止。没有 execution handle 的 pending Replica 可以直接停止。

## Health、Autoscaling、Metrics 与 Usage

Health controller 默认要求连续 3 次失败才标记 unhealthy；一次成功可恢复 healthy。Reconciler 会 drain unhealthy Replica 并创建 replacement。

Autoscaler 复用 Phase II 的 `active requests / target_concurrency`、min/max 和 cooldown。当前 live concurrency source 是 API 进程内聚合；数据库中的 per-Replica `active_requests` 才是 draining 的一致性依据。

新增 bounded-label Prometheus 指标：

```text
gateway_requests_total
gateway_request_duration_seconds
gateway_requests_in_flight
gateway_errors_total
gateway_time_to_first_token_seconds
gateway_tokens_total
replica_active_requests
replica_health
```

不把 request ID 或 user ID 放入 Prometheus label。

Serving usage 只接受 upstream 实际报告且内部一致的 `prompt_tokens/completion_tokens/total_tokens`；缺失或畸形 usage 保持 null，不估算。Streaming 在完整结束后结算。Project usage 同时返回：

- `allocated_gpu_seconds`：成功请求 wall time × Service GPU 数，是请求归因值；并发请求可能重叠。
- `replica_gpu_seconds`：每个 GPU Replica 从 `container_started_at` 到 `stopped_at` 的区间，与查询窗口裁剪后求和；包含 loading/idle/draining，但不代表 GPU utilization 或账单。

Usage 写入是 best-effort，不阻塞已经成功的推理响应；数据库故障时可能丢失该次 request usage，但 active-request release 使用独立事务。

## 运行 Fake Serving E2E

Linux/WSL 中先启动 PostgreSQL 与 Redis，并应用迁移：

```bash
docker compose up -d postgres redis
DATABASE_URL=postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform \
  uv run alembic upgrade head
```

然后显式把 live concurrency test 指向该数据库：

```bash
LIVE_DATABASE_URL=postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform \
  make test-serving
```

该目标覆盖：真实 Fake HTTP、non-streaming、SSE 分块与取消、RR、kill/replacement、2→4→1、in-flight drain、两个 Project/API Key 隔离、并发 Reconciler，以及 Fake GPU TP=4 单节点 gang placement。未提供 `LIVE_DATABASE_URL` 且本机 PostgreSQL 不可达时，live PostgreSQL case 会 skip，因此不能把这种运行记为完整通过。

完整容器栈需要先按 README 配置本地 `.env`，其中至少要有非空的本地 `API_KEY_PEPPER`：

```bash
cp .env.example .env
docker compose up --build --wait -d
curl -fsS http://127.0.0.1:8000/readyz
```

注册 Fake model 后可仅用 Registry ID 部署：

```json
{
  "name": "fake-chat",
  "registered_model_id": "<registered-model-uuid>",
  "replicas": 2
}
```

缩放接口：

```http
POST /api/v1/services/<service-id>/scale
Authorization: Bearer <project-api-key>
Content-Type: application/json

{"replicas": 4}
```

## 真实 vLLM 前置条件

以下条件全部满足后，才能验证 Docker vLLM：

1. Linux dedicated serving node、NVIDIA driver、可用 GPU、NVIDIA Container Toolkit 与 Docker GPU runtime。
2. API/controller 能访问该节点 Docker Engine；不要在默认公网 API 容器上直接增加 socket 权限。
3. `SERVICE_VLLM_DOCKER_ENABLED=true`，配置稳定且唯一的 `SERVICE_VLLM_WORKER_ID`。
4. `VLLM_IMAGE` 或 Service image 使用经 Project image policy 允许的不可变 digest。
5. `SERVICE_VLLM_ENDPOINT_HOST` 是 Gateway 可达地址，发布端口和网络 ACL 只开放必要范围。
6. 实际验证模型加载、CUDA/NCCL、OOM、tensor parallel、health、流式和 replacement。

本仓库当前只有 vLLM spec/controller/unit tests 和 Docker adapter 代码；本机没有 NVIDIA GPU，不能声称真实 vLLM 已通过。

## 已知限制

- 只实现单节点 tensor parallel，不支持跨节点 TP、NVLink/PCIe 拓扑评分、MIG production policy。
- 每个 vLLM controller 只对本节点 inventory 做 gang placement；多节点 controller 可通过数据库锁竞争领取 Replica，但没有全局最优 serving placement。
- Phase IV-A 已增加 Kubernetes Fake Model Serving controller 和 Kind E2E 工具，详情见 [Phase IV-A Kubernetes 原生模型服务](phase4-kubernetes-serving.md)。它不是 Operator，也不运行 Kubernetes vLLM/GPU workload。
- Service 默认 private；没有 public model marketplace。
- Gateway 不是 OpenAI API 100% feature parity，也没有自研 tokenizer 或推理引擎。
- process-local autoscaling metrics 尚未做跨 API 实例聚合。
- 没有真实 NVIDIA/vLLM、HA 数据层或公网安全部署证据。Kind serving 是否在当前机器实际执行，以 [Phase IV-A 验证报告](verification-report-phase4a-2026-08-24.md) 为准。
