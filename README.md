# Mini Docker Cloud

Mini Docker Cloud 是一个可以完整运行的分布式 Docker 任务平台雏形。客户端通过 HTTP API 提交镜像和命令，API 把任务写入 PostgreSQL，Worker 再通过本机 Docker Engine 启动受限容器。Redis Streams 负责低延迟通知，PostgreSQL 始终保存任务状态、执行权和日志。

这个项目侧重控制面、执行面和故障恢复的边界，适合本地开发、教学和架构验证。它没有认证、租户隔离、镜像准入、远程 Worker 信任体系和独立的容器沙箱，不应直接暴露到公网或处理真实秘密。

## 最短启动路径

需要 Docker Engine、Docker Compose v2，以及能访问 `/var/run/docker.sock` 的 Linux 环境。运行宿主侧验证脚本还需要 Python 3.12 和 `uv`。Windows 上若 Docker 只安装在 WSL 中，请在 WSL 终端执行下面的命令。第一次启动会拉取 PostgreSQL、Redis、Python 基础镜像并构建应用镜像，耗时取决于网络。

```bash
cp .env.example .env
docker compose config
docker compose up --build -d
docker compose ps
curl -sS http://localhost:8000/health
```

`migrate` 服务会在 API 和 Worker 启动前执行 `alembic upgrade head`。只有 PostgreSQL 和 Redis 都可用时，`/health` 才返回 HTTP 200 和 `status=ok`；依赖异常时返回 HTTP 503 和 `status=degraded`。

完整链路可以用自带脚本验证：

```bash
uv run python scripts/e2e_demo.py
```

脚本会等待在线 Worker，提交一个 `python:3.12-slim` 任务，通过 SSE 实时接收 stdout、stderr 与 end 事件，再断言终态、退出码、`execution_id` 和 PostgreSQL 持久日志顺序。停止整套服务时，`make down` 或 `docker compose down` 会保留 PostgreSQL 与 Redis 卷。

项目位于 WSL 的 `/mnt/c`、`/mnt/d` 等 Windows 挂载盘时，`uv` 会退回文件复制，首次安装和 mypy 缓存建立会比 WSL 原生文件系统慢；可在命令前设置 `UV_LINK_MODE=copy` 消除 hardlink 警告。这只影响开发工具耗时，不影响 Compose 运行。

## 架构

```text
curl / CLI / scripts
         |
         v
 +------------------+        SQL transaction        +------------------+
 | FastAPI API      | ----------------------------> | PostgreSQL       |
 | task CRUD / SSE  |      task + outbox event       | source of truth  |
 +--------+---------+                                +---------+--------+
          |                                                    ^
          | outbox dispatcher                                  |
          v                                                    | claim, lease,
 +------------------+       at-least-once notification         | status, logs
 | Redis Streams    | -----------------------------------------+
 | ready + log wake |                                          |
 +--------+---------+                                          |
          |                                                    |
          v                                                    |
 +------------------+       Docker Engine API          +-------+---------+
 | trusted Worker   | -------------------------------> | task container  |
 | scheduler/runtime|       /var/run/docker.sock        | untrusted code  |
 +------------------+                                  +-----------------+
```

| 组件 | 职责 |
| --- | --- |
| API | 校验请求、实现幂等创建、查询和取消任务、读取持久日志、提供 SSE、健康检查与指标 |
| Control Plane | 扫描事务 outbox、发布 Redis 通知、回收过期 lease、释放到期重试、标记离线 Worker |
| PostgreSQL | 保存任务、Worker、日志和 outbox，是状态真相来源 |
| Redis | 用 Consumer Group 提醒 Worker 有任务可取，并为实时日志流提供唤醒信号 |
| Worker | 注册能力、心跳续租、原子领取任务、管理 Docker 容器、写回日志和终态 |
| Docker Engine | 拉取镜像，创建、启动、停止和删除任务容器 |

PostgreSQL 和 Redis 默认只绑定到宿主机 `127.0.0.1`。API 也默认发布到 `127.0.0.1:8000`，可以通过 `API_BIND_ADDRESS` 调整。Compose 默认运行一个 Worker；`WORKER_ID` 留空时，每个副本会按 hostname 和随机后缀生成唯一身份。共享同一 Docker Engine 的多套部署必须设置不同的 `CLUSTER_ID`，孤儿清理只扫描本集群标签的容器。

## How a Task Flows Through the System

1. API 校验镜像名、命令数组、环境变量、资源限制和调度标签。
2. 一次 PostgreSQL 事务同时创建 `tasks` 记录和 `task.ready` outbox 事件，任务从 `pending` 进入 `queued`。事务失败时，两条记录都不会留下。
3. Control Plane 领取未处理的 outbox 事件，将通知写入 Redis Stream。发布失败会释放 outbox 锁并按指数退避重试。
4. Worker 从 Redis Consumer Group 收到 `task_id`。Redis 不可用或消息已经丢失时，Worker 会有限扫描 PostgreSQL 中的 `queued` 任务。
5. Worker 在 PostgreSQL 事务里锁定任务和自身容量，核对并预留 CPU、内存、GPU 与并发槽位，校验标签，然后生成新的 `execution_id` 和 lease。分布式 deadline 使用 PostgreSQL 时钟。
6. 执行器依次进入 `assigned -> pulling -> running`，拉取镜像并创建受限任务容器。Docker 调用只存在于 `DockerRuntime`。
7. Worker 在启动容器前先附着日志流；stdout、stderr 原始小碎片按流有界合并，最迟约 250 ms 刷新，再与系统事件按单调递增的 sequence 写入 PostgreSQL。Redis 日志 Stream 只负责唤醒 SSE 读取循环，实时链路中断不会抹掉持久日志。单次执行的任务输出有总量上限，超过上限会停止容器。
8. Worker 等待退出、取消或超时，停止并删除容器，再用当前 `worker_id + execution_id` 写回终态和资源用量。
9. Worker 心跳丢失且 lease 过期后，Reaper 撤销旧执行权。任务经过短暂清理宽限期后进入恢复重试；同一 Docker Engine 上的健康 Worker 会先清理 execution token 已失效的托管容器。超过 `MAX_RECOVERY_ATTEMPTS` 后任务稳定为 `failed`。

任务状态如下：

```text
pending -> queued -> assigned -> pulling -> running
             ^          |          |          |
             |          +----------+----------+
             |                failure/timeout
             |
          retrying

terminal: succeeded | failed | cancelled | timed_out
```

失败或超时任务在 `retry_count < max_retries` 时进入 `retrying`，退避到期后重新排队。运行中的取消先设置 `cancel_requested`，Worker 观察到标记后停止真实容器并写入 `cancelled`。

## At-least-once 与 execution_id 边界

Redis 通知采用 at-least-once 语义，同一个 `task_id` 可能出现多次。Worker 不把消息本身当作执行权，它必须回到 PostgreSQL 原子 claim。第一个成功把 `queued` 改为 `assigned` 的 Worker 获得任务，之后到达的重复消息只能得到一次无效 claim，并被安全确认。

每一轮合法分配都会生成新的 `execution_id`。续租、日志追加和结果写回都会检查当前 Worker 与 `execution_id`。场景如下：

```text
Worker A claims task, execution_id=A1
A loses heartbeat and its lease expires
Reaper revokes A1
Worker B claims task, execution_id=B1
A later reports success for A1
repository rejects the stale result
```

这道 fencing 只保护控制面的最终状态，无法撤销已经发到外部系统的副作用。用户任务如果会发送邮件、扣款或写外部数据库，仍需在任务内部使用业务幂等键。API 的 `Idempotency-Key` 解决的是重复提交，它和 Worker 的 `execution_id` 分工不同：

- 相同 `Idempotency-Key` 与相同请求体返回同一个任务。
- 相同 key 配上不同请求体返回 `409 IDEMPOTENCY_KEY_REUSED`。
- 没有 key 的两个 POST 会创建两个独立任务。
- `execution_id` 每次重新分配都会变化，旧执行无法覆盖新执行。

## 安全边界

Worker 是可信基础设施组件。它挂载 `/var/run/docker.sock`，拥有控制宿主 Docker Engine 的能力，这个权限通常等价于宿主机 root。只应运行仓库内受审查的 Worker 代码，不要把 Worker 容器当作普通业务容器，也不要把 socket 挂给 API 或用户任务。

用户任务按不可信代码处理，当前运行时固定启用以下约束：

- `privileged=false`，丢弃全部 Linux capabilities，并设置 `no-new-privileges`。
- 根文件系统只读，只提供带大小上限且禁执行的 `/tmp` tmpfs。
- 默认 `network_mode=none`，请求必须显式设置 `network_enabled=true` 才能使用 bridge 网络。
- 设置 CPU、内存和 PID 上限，关闭 stdin 与 TTY，并使用 init 进程回收子进程。
- 请求模型只接受预定义字段，不能传任意 Docker 参数、挂载、设备、端口、privileged 或 socket。
- 任务容器带 task 与 execution 标签，结束后强制删除。

GPU 请求会转换为 NVIDIA device request。宿主机没有 NVIDIA Container Runtime 时，任务会以明确错误失败，不会自动退回 CPU。

仍需明确几个缺口：任务环境变量会写入 PostgreSQL，也会在任务查询响应中返回，所以不要用它传真实 secret；bridge 网络没有出站域名策略；当前没有自定义 seccomp、AppArmor、镜像签名验证、镜像仓库白名单、恶意镜像扫描、认证和限流。公网或多租户部署需要在这些边界补齐后再评估。

## HTTP API

API 基址默认是 `http://localhost:8000`。FastAPI 交互文档位于 `/docs`，OpenAPI JSON 位于 `/openapi.json`。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/v1/tasks` | 创建任务，支持 `Idempotency-Key` |
| GET | `/api/v1/tasks` | 按状态或 Worker 过滤并分页 |
| GET | `/api/v1/tasks/{task_id}` | 查询状态、执行权、资源用量与成本 |
| POST | `/api/v1/tasks/{task_id}/cancel` | 取消未完成任务 |
| GET | `/api/v1/tasks/{task_id}/logs` | 分页读取 PostgreSQL 持久日志 |
| GET | `/api/v1/tasks/{task_id}/logs/stream` | 通过 SSE 跟随日志 |
| GET | `/api/v1/workers` | 分页列出 Worker 与可用容量 |
| GET | `/api/v1/workers/{worker_id}` | 查询一个 Worker |
| GET | `/health` | 检查 PostgreSQL 与 Redis |
| GET | `/metrics` | Prometheus 文本指标 |

### 创建任务

```bash
curl -sS -X POST http://localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-hello-001" \
  -d '{
    "image": "python:3.12-slim",
    "command": [
      "python",
      "-c",
      "import time; print(\"hello\", flush=True); time.sleep(2); print(\"goodbye\")"
    ],
    "environment": {"DEMO_MODE": "true"},
    "timeout_seconds": 30,
    "max_retries": 1,
    "cpu_limit": 0.5,
    "memory_limit_mb": 128,
    "gpu_count": 0,
    "network_enabled": false,
    "labels": {}
  }'
```

成功响应为 HTTP 201：

```json
{
  "id": "7da5e462-2285-4d66-b447-da332e934c8f",
  "status": "queued"
}
```

创建字段受严格校验，额外字段会返回 422：

| 字段 | 默认值 | 约束 |
| --- | --- | --- |
| `image` | 必填 | 非空，最长 512，不允许空白或控制字符 |
| `command` | 必填 | 1 到 256 个字符串，按 argv 原样传给 Docker，不经 shell 拼接 |
| `environment` | `{}` | 最多 256 项，变量名必须符合常见环境变量格式 |
| `timeout_seconds` | 60 | 1 到 86400，还受 `MAX_TASK_TIMEOUT` 限制 |
| `max_retries` | 0 | 0 到 100，还受 `MAX_TASK_RETRIES` 限制 |
| `cpu_limit` | 1.0 | 大于 0，最多 1024 |
| `memory_limit_mb` | 256 | 16 到 1048576 |
| `gpu_count` | 0 | 0 到 64 |
| `network_enabled` | false | 必须是 JSON boolean |
| `labels` | `{}` | 最多 64 项，Worker 必须提供全部请求标签 |

### 查询、分页和取消

将创建响应里的 UUID 设为 `TASK_ID`：

```bash
TASK_ID="7da5e462-2285-4d66-b447-da332e934c8f"

curl -sS "http://localhost:8000/api/v1/tasks/$TASK_ID"
curl -sS "http://localhost:8000/api/v1/tasks?status=running&limit=20&offset=0"
WORKER_ID="<从 /api/v1/workers 响应复制的 Worker ID>"
curl -sS "http://localhost:8000/api/v1/tasks?worker_id=$WORKER_ID&limit=20"
curl -sS -X POST "http://localhost:8000/api/v1/tasks/$TASK_ID/cancel"
```

取消 `running` 任务时，响应可能暂时仍是 `running`，但 `cancel_requested=true`。Worker 停掉容器并提交终态后才会变成 `cancelled`。已经 `succeeded` 的任务不能取消，API 返回 409。

### 持久日志和 SSE

```bash
curl -sS "http://localhost:8000/api/v1/tasks/$TASK_ID/logs?offset=0&limit=500"
curl -N "http://localhost:8000/api/v1/tasks/$TASK_ID/logs/stream?offset=0"
curl -N \
  -H "Last-Event-ID: 12" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/logs/stream"
```

SSE 会发送 `log`、`end` 和 `error` 事件，并定期发送注释心跳。重连时可把最后一个日志 sequence 放进 `Last-Event-ID`。PostgreSQL 保存受 `MAX_TASK_LOG_BYTES` 约束的持久日志；Redis 只保存 sequence 唤醒信号，执行结束后主动删除，异常时再由长度上限和滑动 TTL 兜底。

### Worker、健康和指标

```bash
curl -sS "http://localhost:8000/api/v1/workers?limit=100&offset=0"
WORKER_ID="<从上一个响应复制的 Worker ID>"
curl -sS "http://localhost:8000/api/v1/workers/$WORKER_ID"
curl -sS http://localhost:8000/health
curl -sS http://localhost:8000/metrics
```

Worker 响应包含心跳时间、状态、标签、Docker 版本、GPU 信息、资源预留和 `capacity.available_slots`。`/metrics` 暴露任务创建与终态计数、queued/running/online Worker gauge，以及队列等待和执行耗时 histogram。

### 错误格式与请求 ID

所有错误使用同一结构，响应头同时返回 `X-Request-ID`。客户端可以传入不超过 255 字符的 `X-Request-ID`，方便关联结构化日志。

```json
{
  "error": {
    "code": "TASK_NOT_FOUND",
    "message": "Task not found",
    "request_id": "d89b69aa-b9cb-486c-a82f-232a65be12e7"
  }
}
```

常见状态码为 404、409、422、500 和 503。`details` 只在冲突或字段校验需要更多上下文时出现。

## CLI

安装开发依赖后，可以用 Typer CLI 调用同一套 HTTP API。`MINI_DOCKER_CLOUD_URL` 用来切换目标地址。

```bash
uv sync --all-groups
export MINI_DOCKER_CLOUD_URL="http://localhost:8000"

uv run mini-docker-cloud submit \
  --image python:3.12-slim \
  --command "python -c \"print('hello from cli')\"" \
  --env DEMO_MODE=true \
  --label runtime=docker \
  --timeout-seconds 30 \
  --max-retries 1 \
  --cpu-limit 0.5 \
  --memory-limit-mb 128 \
  --gpu-count 0 \
  --idempotency-key cli-demo-001

TASK_ID="<submit 返回的 UUID>"
uv run mini-docker-cloud status "$TASK_ID"
uv run mini-docker-cloud logs "$TASK_ID"
uv run mini-docker-cloud logs --follow "$TASK_ID"
uv run mini-docker-cloud cancel "$TASK_ID"
uv run mini-docker-cloud workers
```

传入 `--network-enabled` 会为任务启用 bridge 网络。`--env` 和 `--label` 可以重复多次，格式都是 `KEY=VALUE`。`--command` 由 CLI 用 `shlex` 拆成 argv；API 本身始终接收字符串数组。

## 配置

复制 `.env.example` 后再修改。示例密码只供本机开发，部署到共享环境前必须替换。Compose 内部地址使用服务名 `postgres` 和 `redis`；从宿主机运行 API、Alembic 或测试时，应把 URL 主机改成 `localhost`。

### Compose 与连接

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_IMAGE_TAG` | local | 应用镜像标签 |
| `POSTGRES_DB` | task_platform | Compose PostgreSQL 数据库 |
| `POSTGRES_USER` | task | Compose PostgreSQL 用户 |
| `POSTGRES_PASSWORD` | local-dev-only | 仅供本地的示例密码 |
| `POSTGRES_PORT` | 5432 | 绑定到宿主 `127.0.0.1` 的端口 |
| `DATABASE_URL` | `postgresql+asyncpg://...@postgres:5432/task_platform` | API、Worker 和 Alembic 使用的异步 URL |
| `REDIS_PORT` | 6379 | 绑定到宿主 `127.0.0.1` 的端口 |
| `REDIS_URL` | `redis://redis:6379/0` | Redis Stream 与日志唤醒连接 |
| `API_BIND_ADDRESS` | 127.0.0.1 | API 发布到宿主机的地址 |
| `API_HOST` | 0.0.0.0 | 宿主模式启动 API 时使用 |
| `API_PORT` | 8000 | API 发布端口 |
| `LOG_LEVEL` | INFO | 结构化日志级别 |
| `CONTROL_PLANE_ENABLED` | true | 是否在 API 进程启动 outbox 与 Reaper 循环 |
| `CLUSTER_ID` | mini-docker-cloud-local | Docker 托管容器的部署命名空间；共享 daemon 时必须唯一 |

### Worker、lease 与 Docker

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKER_ID` | 空 | 留空时为每个进程生成唯一 ID；显式设置时多 Worker 必须保证唯一 |
| `WORKER_CONCURRENCY` | 4 | 单 Worker 并发任务上限 |
| `WORKER_LABELS` | `runtime=docker,region=local` | 逗号分隔的调度标签，也接受 JSON object |
| `HEARTBEAT_INTERVAL` | 5 | Worker 心跳节拍，必须小于离线阈值 |
| `WORKER_OFFLINE_TIMEOUT` | 15 | 心跳超过此时间后标记离线 |
| `TASK_LEASE_SECONDS` | 30 | 一轮 execution 的租约长度 |
| `LEASE_RENEW_INTERVAL` | 5 | lease 续租节拍，必须小于 lease 长度；Worker 使用它与心跳节拍中的较小值 |
| `WORKER_SHUTDOWN_TIMEOUT` | 30 | 优雅退出等待在途任务的时间 |
| `WORKER_STOP_GRACE_PERIOD` | 40s | Compose 给 Worker 的停止宽限期 |
| `MAX_RECOVERY_ATTEMPTS` | 3 | lease 过期后的恢复次数上限 |
| `RECOVERY_CLEANUP_GRACE_SECONDS` | 5 | 旧 execution 撤销后、重新排队前的容器清理宽限期 |
| `DOCKER_STOP_TIMEOUT` | 5 | Docker stop 的宽限时间 |
| `DOCKER_ALWAYS_PULL` | false | false 时优先复用本机镜像；缺失时仍自动 pull，true 时每轮强制刷新 |
| `DOCKER_PIDS_LIMIT` | 256 | 每个任务容器的 PID 上限 |
| `DOCKER_TMPFS_SIZE_MB` | 64 | 任务 `/tmp` 的大小上限 |
| `ORPHAN_RECONCILE_INTERVAL` | 1 | Worker 扫描失效托管容器的周期 |
| `MAX_TASK_LOG_BYTES` | 10485760 | 单次执行可持久化的 stdout/stderr 总字节数 |
| `MAX_LOG_CHUNK_BYTES` | 65536 | 单条持久日志的最大分片字节数 |
| `LOG_DRAIN_TIMEOUT` | 5 | 容器停止后等待日志流收口的时间 |

### 重试、控制面、日志与计费雏形

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DEFAULT_TASK_TIMEOUT` | 60 | 应用默认超时配置 |
| `MAX_TASK_TIMEOUT` | 86400 | API 允许的任务超时上限 |
| `MAX_TASK_RETRIES` | 10 | API 允许的重试次数上限 |
| `RETRY_MAX_BACKOFF_SECONDS` | 60 | 失败重试的最大退避 |
| `OUTBOX_POLL_INTERVAL` | 0.25 | outbox 扫描周期 |
| `SCHEDULER_POLL_INTERVAL` | 1 | Worker 无消息时的调度扫描周期 |
| `REAPER_INTERVAL` | 5 | 离线 Worker 与过期 lease 扫描周期 |
| `CONTROL_OPERATION_TIMEOUT` | 30 | 单次控制面后台操作 deadline |
| `CONTROL_SHUTDOWN_TIMEOUT` | 10 | API 停止控制面循环的 deadline |
| `HEALTH_CHECK_TIMEOUT` | 3 | 单个健康依赖探测 deadline |
| `BATCH_SIZE` | 100 | outbox、恢复和数据库 fallback 的批量上限 |
| `LOG_STREAM_MAXLEN` | 10000 | 每个 Redis 日志 Stream 的近似长度上限 |
| `LOG_STREAM_TTL_SECONDS` | 86400 | 每次实时日志写入后刷新该任务 Redis Stream 的滑动 TTL |
| `READY_STREAM_MAXLEN` | 100000 | ready Stream 的近似长度上限；PostgreSQL fallback 防止 trim 丢任务 |
| `REDIS_SOCKET_TIMEOUT` | 5 | Redis 连接与命令 socket timeout |
| `SSE_HEARTBEAT_SECONDS` | 10 | SSE 注释心跳周期 |
| `CPU_PRICE_PER_HOUR` | 0.05 | 教学用 CPU 估价 |
| `GPU_PRICE_PER_HOUR` | 1.0 | 教学用 GPU 估价 |

`estimated_cost` 只是按运行时间和配置单价计算的模拟值，不代表账单或精确资源计量。

## 开发与测试

Python 要求 3.12 及以上，宿主开发环境使用 `uv.lock` 固定依赖。常用命令如下：

```bash
make install
make lint
make typecheck
make test
make test-unit
make test-integration
make test-docker
make test-e2e
make config
```

| 命令 | 外部条件 |
| --- | --- |
| `make test-unit` | 不需要 PostgreSQL、Redis 或 Docker daemon |
| `make test-integration` | 总会运行 SQLite/fakeredis 隔离测试；若本机 Compose PostgreSQL/Redis 可达，还会自动验证真实行锁并发 claim 与 Consumer Group，否则明确 skip |
| `make test-docker` | 需要可访问的 Docker Engine，并可能拉取 `alpine:3.21` |
| `make test-e2e` | 运行真实 Docker Runtime E2E；完整服务栈使用下方 `e2e_demo.py` |
| `make lint` / `make typecheck` | 需要先执行 `make install` |

Compose 配置可以在不启动容器的情况下检查：

```bash
docker compose config --quiet
docker compose --env-file .env.example config --quiet
```

真实后端测试默认连接 `127.0.0.1:5432` 与 `127.0.0.1:6379`，也可通过 `LIVE_DATABASE_URL`、`LIVE_REDIS_URL` 指向专用测试服务。它只创建带随机 UUID 的行和 Redis key，并在 `finally` 中精确清理。

### E2E 演示

```bash
uv run python scripts/e2e_demo.py \
  --base-url http://localhost:8000 \
  --timeout 180
```

成功时脚本输出任务 ID、Worker ID、`execution_id`、日志条数和 `assertions: passed`。任一状态、退出码或日志断言失败都会返回非零退出码。

### 100 任务负载测试

```bash
uv run python scripts/load_test.py
```

默认并发提交 100 个短任务，并输出 JSON：

- `submit_throughput_tasks_per_second`：成功创建数除以提交阶段用时。
- `completion_throughput_tasks_per_second`：到达终态的任务数除以首个请求到最后终态的观察窗口。
- `latency_seconds`：从单个 POST 开始到终态的 avg、P50、P95、P99。
- `success_rate_percent`：`succeeded / requested`。
- `statuses`、`submit_errors` 和 `poll_errors`：保留失败分布与客户端超时原因。

可调参数：

```bash
uv run python scripts/load_test.py \
  --count 100 \
  --submit-concurrency 20 \
  --poll-concurrency 50 \
  --completion-timeout 300 \
  --min-success-rate 99
```

默认最低成功率是 100%，任务未全部到达终态或成功率低于阈值时，脚本返回 1。平台不可用或健康检查失败时返回 2。这里的结果只代表当前机器、镜像缓存和参数，不能直接当作生产容量。

## 故障注入

`scripts/fault_injection.ps1` 会停止或 SIGKILL Compose 服务、直接写入测试用 Redis 消息、修改测试任务的 lease，并清理带对应 task 标签的容器。只在一次性本地开发栈运行，不要对共享或生产数据库执行。脚本使用 PowerShell `ShouldProcess`，默认会显示确认提示；中途强制终止后，应手动检查服务和残留任务容器。

列出场景不会改动运行状态：

```powershell
.\scripts\fault_injection.ps1 -Case List
```

单独执行和连续执行：

```powershell
.\scripts\fault_injection.ps1 -Case RedisUnavailable -Confirm:$false
.\scripts\fault_injection.ps1 -Case StaleWorkerResult -Confirm:$false
.\scripts\fault_injection.ps1 -Case All -Confirm:$false
```

Windows PATH 中没有 Docker CLI、但 WSL 内有 Docker 时，脚本会自动通过 `wsl.exe` 调用 Docker。可以用 `-BaseUrl`、`-WaitTimeoutSeconds`、`-LeaseWaitSeconds`、`-PostgresUser` 和 `-PostgresDatabase` 调整现场参数。

| Case | 参数 | 注入与断言 |
| --- | --- | --- |
| 1 | `RedisUnavailable` | 停 Redis，提交任务，断言 PostgreSQL fallback 仍能完成，再恢复 Redis |
| 2 | `PostgresUnavailable` | 停 PostgreSQL，断言 DB 查询失败且健康状态 degraded，再恢复数据库 |
| 3 | `ImagePullFailure` | 提交不存在的镜像标签，断言任务 failed 且保存拉取错误 |
| 4 | `CommandExitOne` | 执行 `sys.exit(1)`，断言 failed 和 `exit_code=1` |
| 5 | `TaskTimeout` | 运行超时命令，断言 timed_out 且任务容器已删除 |
| 6 | `WorkerDeath` | 任务运行时 SIGKILL Worker，等待 lease 回收并重启，断言恢复执行成功 |
| 7 | `ApiRestart` | 任务运行时重启 API，断言 Worker 执行不受影响 |
| 8 | `DuplicateEnqueue` | 向 `tasks:ready` 重复写入同一 task，断言只有一次容器启动 |
| 9 | `StaleWorkerResult` | 撤销旧 lease、生成新 execution，再调用旧 execution 写回，断言 `accepted=false` |
| 10 | `CancelRunning` | 取消 running task，断言 cancelled 且真实容器消失 |

故障脚本会在场景结束时恢复被停掉的服务。失败或手动中断后可执行：

```bash
docker compose up -d postgres redis api worker
docker compose ps
docker ps -a --filter label=mini-docker-cloud.managed=true
```

## 运行与排障

```bash
make up
make ps
make logs
make migrate
docker compose logs --tail=200 api worker migrate
curl -sS http://localhost:8000/metrics
```

Redis 故障时，任务通知和实时日志唤醒会延迟，但 PostgreSQL fallback 与持久日志仍可工作。PostgreSQL 故障时，API 无法可靠创建或查询任务，Worker 也不能领取和提交状态。Docker image pull、create、start 或 wait 失败会记录到任务错误，并按任务重试配置处理。

Worker 正常收到 SIGTERM 后会进入 `draining`，等待在途任务到 `WORKER_SHUTDOWN_TIMEOUT`，随后停止剩余容器并标记离线。Worker 被 SIGKILL 时没有清理机会，Reaper 依靠心跳和 lease 撤销旧 execution；同一 Docker Engine 上的其他 Worker 会在领取新任务前清理失效托管容器。跨主机网络分区时无法远程强制停止失联主机上的容器，因此带外副作用仍必须由任务自身做业务幂等。

数据库迁移可以单独运行：

```bash
make migrate
# 或者宿主环境已设置 DATABASE_URL 时：
make migrate-local
```

`make down` 保留数据卷。下面的命令会永久删除本项目的 PostgreSQL 和 Redis 本地数据，只能在确定不需要数据时执行：

```bash
docker compose down --volumes
```

## 目录

```text
api/                 FastAPI routes, schemas, services
cli/                 Typer HTTP client
core/                config, database, Redis, logging, metrics, state machine
models/              SQLAlchemy models
repositories/        transactional persistence and fencing
scheduler/           capacity and label-aware task claim
worker/              heartbeat, executor, Docker runtime
migrations/          Alembic environment, initial schema and capacity migration
docker/              application image
scripts/             E2E, load and fault injection tools
tests/               unit, integration, Docker and E2E tests
docker-compose.yml   local complete stack
```

## 当前边界

这个雏形已经把数据库真相、事务 outbox、at-least-once 通知、lease、执行 fencing、持久日志、取消和容器清理串成一条链路。面向真实生产环境仍需增加身份认证与授权、租户配额、速率限制、secret 管理、镜像准入与签名、细粒度网络策略、独立 Worker 节点、审计、告警、高可用数据库和比 Docker socket 更强的隔离方案。
