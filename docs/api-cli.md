# API 与 CLI 使用

API 基址默认 `http://localhost:8000`。除 `/livez`、`/readyz`、`/health`、`/metrics` 和一次性 bootstrap 外，Phase II 资源使用 API Key：

```http
Authorization: Bearer mkc_...
```

也支持 `X-API-Key`，但同一请求不能同时传两种认证头。API Key 与 Project 绑定，不能通过请求参数切换 tenant。

## 统一错误

业务错误使用稳定 envelope，并在响应头回传 `X-Request-ID`：

```json
{
  "error": {
    "code": "TASK_NOT_FOUND",
    "message": "Task not found",
    "request_id": "9ceeb423-d830-443e-a9b1-3d026b4ad0d4",
    "details": {}
  }
}
```

常见状态码：

| 状态 | 含义 |
| --- | --- |
| 400 | 认证头冲突、请求语义错误 |
| 401 | API Key 缺失/无效/过期/撤销 |
| 403 | principal 已认证但缺少 RBAC permission |
| 404 | 资源不存在；跨 Project 也使用 not-found semantics |
| 409 | 状态、quota、policy 或幂等冲突 |
| 413 | JSON/body/artifact 超过上限 |
| 422 | schema 或完整性校验失败 |
| 429 | API Key minute bucket 超限 |
| 503 | PostgreSQL/Redis/rate-limit/secret/object-store 等依赖不可用 |

## Endpoint 索引

以运行实例的 `/openapi.json` 为最终准绳。主要接口：

| 资源 | 方法与路径 |
| --- | --- |
| Identity | `POST /api/v1/bootstrap`, `GET /api/v1/auth/whoami`, `POST /api/v1/users` |
| Projects | `POST/GET /api/v1/projects`, `GET /api/v1/projects/current`, membership 与 API key 子资源 |
| Tasks | `POST/GET /api/v1/tasks`, `GET /api/v1/tasks/{id}`, cancel、timeline、logs、SSE |
| Workers | `GET /api/v1/workers`, `GET /api/v1/workers/{id}` |
| Quota/Usage | `GET/PUT /api/v1/projects/{id}/quota`, `GET .../usage`, `GET .../cost` |
| Registry | `/api/v1/projects/{id}/models`, `/secrets`, `/image-policy` |
| Artifacts | `/api/v1/artifacts`，upload/download grant、content、finalize、delete |
| Datasets | `/api/v1/projects/{id}/datasets` 与 immutable version 子资源 |
| DAG | `/api/v1/projects/{id}/job-groups` 与 dependency/ready-state 子资源 |
| Services | `/api/v1/services`，scale、stop、replicas |
| Gateway | `GET /v1/models`, `POST /v1/chat/completions`, `POST /v1/completions` |
| Events | `WS /api/v1/projects/{project_id}/events/ws`，支持 cursor 恢复 |
| Admin | `GET /api/v1/admin/diagnostics`、`POST .../diagnostics/repair`，只允许具备 audit permission 的 principal |

列表接口限制 `limit` 上限。兼容接口可能仍返回 `offset` metadata；支持 cursor 的接口应优先使用服务端返回的下一游标，不要自己构造游标。

## Task

普通 Docker CPU 请求继续兼容 Phase I：

```bash
curl -fsS -X POST http://localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: training-2026-08-23-001' \
  -d '{
    "workload_type": "batch_job",
    "runtime_type": "docker",
    "image": "docker.io/library/python:3.12-slim",
    "command": ["python", "-c", "print(\"training\")"],
    "environment": {"RUN_MODE": "demo"},
    "timeout_seconds": 60,
    "retry_policy": {
      "max_attempts": 2,
      "backoff": "exponential",
      "base_seconds": 2.0,
      "max_seconds": 60.0,
      "retry_on_exit_codes": [1, 137]
    },
    "cpu_limit": 1.0,
    "memory_limit_mb": 512,
    "gpu_count": 0,
    "priority": 50,
    "preemptible": false,
    "network_enabled": false,
    "labels": {"region": "local"},
    "tolerations": [],
    "inputs": [],
    "artifacts": [],
    "depends_on": [],
    "dependency_failure_policy": "cancel"
  }'
```

镜像必须满足 Project image policy。默认新项目 deny 且要求 digest；本地演示允许 tag 的最小策略见根 README。

`max_attempts` 包含第一次执行，旧字段 `max_retries=N` 等价于 `max_attempts=N+1`。`infra_error`、`internal_error` 和 `timeout` 在预算内自动重试；`user_error`、`resource_error` 只有 exit code 命中 `retry_on_exit_codes` 才重试；取消不重试，抢占重排不消耗 retry budget。Task 与每次 execution 都保存稳定 `error_category/error_code`；`OOMKilled` 或 exit 137 映射为 `resource_error/OOM_KILLED`，是否重试仍由 policy 决定。

查询和日志：

```bash
TASK_ID="<创建响应 id>"
curl -fsS -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID"
curl -fsS -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/timeline"
curl -fsS -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/logs?limit=500"
curl -N -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/logs/stream"
curl -fsS -X POST -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/cancel"
```

SSE 重连可发送 `Last-Event-ID`。Redis stream 只负责唤醒，持久日志仍从 PostgreSQL 读取。

## Project quota、usage 与 cost

```bash
curl -fsS -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/projects/$PROJECT_ID/quota"

curl -fsS -H "Authorization: Bearer $API_KEY" \
  --get "http://localhost:8000/api/v1/projects/$PROJECT_ID/usage" \
  --data-urlencode 'from=2026-08-23T00:00:00+08:00' \
  --data-urlencode 'to=2026-08-24T00:00:00+08:00'

curl -fsS -H "Authorization: Bearer $API_KEY" \
  --get "http://localhost:8000/api/v1/projects/$PROJECT_ID/cost" \
  --data-urlencode 'from=2026-08-23T00:00:00+08:00' \
  --data-urlencode 'to=2026-08-24T00:00:00+08:00'
```

Cost 是配置价格表上的模拟估价，不是账单。usage 只统计已结算且通过唯一约束防重复的 execution。

## Artifact 上传与下载

Artifact 不塞进大型 JSON；采用 metadata → transfer grant → upload → finalize：

```bash
FILE=./result.bin
SIZE=$(wc -c < "$FILE" | tr -d ' ')
SHA=$(sha256sum "$FILE" | awk '{print $1}')

curl -fsS -X POST http://localhost:8000/api/v1/artifacts \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"result.bin\",\"size_bytes\":$SIZE,\"sha256\":\"$SHA\"}"
```

取创建响应 `id` 后请求 `/upload-url`。`authorization=api` 时把平台 API Key 发送给返回的 API URL；`authorization=presigned` 时绝不能把平台 API Key 发送给对象存储 URL。按 grant 的 method/headers 上传，再调用：

```bash
curl -fsS -X POST "http://localhost:8000/api/v1/artifacts/$ARTIFACT_ID/finalize" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"size_bytes\":$SIZE,\"sha256\":\"$SHA\"}"
```

下载同样先取 `/download-url`。`scripts/phase2_demo.py` 已实现 Local/S3 两种 grant 分支并做下载后 SHA-256 断言。

Task 也接受输入/输出声明，并可查询持久 binding：

```json
{
  "inputs": [{"artifact_id": "11111111-1111-1111-1111-111111111111"}],
  "artifacts": [{"name": "model", "path": "/output/model.bin", "required": true}]
}
```

```bash
curl -fsS -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8000/api/v1/tasks/$TASK_ID/artifacts"
```

Worker 会在 execution-fenced 私有 workspace 中校验并物化 inputs，把声明文件逐一 mount 给 runtime，在成功退出后、terminal transition 前发布 outputs，最后清理 workspace。Compose named-volume `Subpath` 已通过真实 Docker task artifact E2E；裸机单文件 bind、executor 顺序和 Kubernetes pinned file `hostPath` 有自动化测试。Kubernetes 仍只有 Pod spec 证据，没有 Kind 文件可见性实测。

## Secret

Secret API 启用前必须配置 `SECRET_MASTER_KEY`。创建/轮换请求含明文，列表和 GET 响应永不返回明文：

```bash
curl -fsS -X POST "http://localhost:8000/api/v1/projects/$PROJECT_ID/secrets" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"HF_TOKEN","value":"use-a-local-test-secret"}'
```

任务应使用 task schema 中的 `secret_bindings` 引用 Secret id/version/env name，不应把真实 secret 放入普通 `environment`。

## Model Service 与 Gateway

```bash
curl -fsS -X POST http://localhost:8000/api/v1/services \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "qwen-demo",
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "runtime": "vllm",
    "runtime_type": "fake",
    "gpu_count": 0,
    "replicas": 2,
    "autoscaling": {
      "enabled": true,
      "min_replicas": 1,
      "max_replicas": 4,
      "target_concurrency": 8,
      "cooldown_seconds": 60
    }
  }'

curl -fsS -H "Authorization: Bearer $API_KEY" http://localhost:8000/v1/models
curl -fsS -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen-demo","messages":[{"role":"user","content":"hello"}]}'
```

Fake runtime 只允许 development/test。Docker vLLM replica controller 默认关闭，只能在具备 Docker socket 与真实 NVIDIA inventory 的专用 serving node 显式启用；无 GPU 环境只验证 fenced lifecycle/spec，不声称模型吞吐或显存实测。

## CLI

`mini-cloud` 是主命令；Phase I 名称 `mini-docker-cloud` 保留一个开发版本作为兼容入口，并在 stderr 输出弃用提示：

```bash
uv run mini-cloud auth login --url http://localhost:8000

uv run mini-cloud project create --name "Research" --slug research
uv run mini-cloud project list

uv run mini-cloud task submit \
  --image python:3.12-slim \
  --command "python -c 'print(123)'"
uv run mini-cloud task list --status queued
uv run mini-cloud task explain "$TASK_ID"
uv run mini-cloud logs --follow "$TASK_ID"
uv run mini-cloud cancel "$TASK_ID"
uv run mini-cloud workers

uv run mini-cloud service deploy \
  --name qwen-demo --model Qwen/Qwen2.5-0.5B-Instruct \
  --runtime vllm --runtime-type fake --replicas 2
uv run mini-cloud service list
uv run mini-cloud service scale "$SERVICE_ID" --replicas 3
uv run mini-cloud service stop "$SERVICE_ID"

uv run mini-cloud usage --project-id "$PROJECT_ID" \
  --from 2026-08-23T00:00:00+08:00 \
  --to 2026-08-24T00:00:00+08:00
uv run mini-cloud admin doctor
uv run mini-cloud admin doctor --repair
```

`admin doctor` 默认只读。`--repair` 只在一个数据库事务中幂等释放 terminal-task reservation 和清除 terminal-task lease；无法证明安全的候选会跳过，不会停止容器/Pod、修负容量或删除 runtime orphan。跨节点 runtime inventory 当前不可见，因此 orphan container/pod 会显式报告 `not_observable`。

不带 `--api-key` 的 `auth login` 使用隐藏 prompt。主环境变量为 `MINI_CLOUD_URL`、`MINI_CLOUD_API_KEY` 和 `MINI_CLOUD_CONFIG`；旧的 `MINI_DOCKER_CLOUD_*` 前缀在本开发版本仍作为低优先级兼容读取。环境变量适合一次性 CI，配置文件适合本地交互。不要在 CI 日志打印 key。
