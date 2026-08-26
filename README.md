# Mini AI Cloud

Mini AI Cloud（仓库名仍为 `mini-docker-cloud`）是一个面向 AI Infra 学习、架构验证和本地演示的多租户计算平台雏形。它在 Phase I 的 Docker 分布式任务链路上增量加入项目/API Key、配额与用量、GPU 感知全局调度、Docker/Kubernetes 运行时、对象存储、DAG、Model Service、OpenAI-compatible Gateway、审计与诊断能力。

这是一套“生产思维的可运行原型”，不是可直接暴露到公网的托管云。Worker 仍是可信基础设施，Docker Worker 挂载 Docker socket，该权限通常等价于宿主机 root。生产部署还需要独立节点身份、mTLS、网络隔离、镜像签名/扫描、HA 数据层和更强沙箱。

## 当前能力

- Batch Job：Phase I 的 `/api/v1/tasks` 保持兼容，支持持久日志、SSE、lease、`execution_id` fencing、重试、取消、超时和恢复。
- 多租户控制面：User、Project、Membership、RBAC、一次性展示的 API Key、并发安全 Project Quota、Usage Ledger 与模拟成本。
- Scheduler v2：CPU/RAM/GPU reservation、具体 GPU device、label/taint/toleration、priority/aging、binpack/spread、project fairness 和两阶段抢占。
- Runtime：统一 `ComputeRuntime` 接口，提供 Docker、Kubernetes、Fake runtime；无 GPU 机器可使用仅限开发/测试的 Fake GPU inventory。
- Artifact：Local/S3（含 MinIO）后端、流式上传下载、大小/配额/SHA-256 与 project isolation；Task input/output 经过 execution-fenced workspace，以单文件只读/可写 mount 交给 runtime，成功后先发布输出再提交终态。
- AI Service：vLLM 规格、Fake inference 完整控制面、replica lease/health/reconciliation/autoscaling，以及 `/v1/models`、`/v1/chat/completions`、`/v1/completions` 代理；另有仅显式启用、面向专用 GPU serving node 的 Docker vLLM replica controller，按 Project/Service 隔离模型缓存并使用具体 GPU UUID。
- 平台资源：模型注册表、AES-256-GCM Secret、镜像策略、任务时间线、Job Group/DAG、Prometheus/Grafana、admin diagnostics 与保守 repair、备份恢复和调度模拟。

实现、已验证项和环境受限项不混为一谈；逐项证据见 [验证矩阵](docs/verification-matrix.md)。

## 最短启动路径

需要 Docker Engine、Docker Compose v2。宿主开发和脚本要求 Python 3.12 与 `uv`。Windows 若 Docker 只在 WSL 中可用，请在 WSL 终端执行。

```bash
cp .env.example .env
docker compose config --quiet
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/readyz
```

`migrate` 服务会先执行 `alembic upgrade head`。`/livez` 只表示 API 进程存活；默认 fail-closed 限流模式下，`/readyz` 同时要求 PostgreSQL 与 Redis 可用。`/health` 会把 Redis 故障报告为 HTTP 200 `degraded`，而 PostgreSQL 故障返回 503；只有显式 `RATE_LIMIT_FAIL_OPEN=true` 时，readiness 才允许 Redis 降级。

Phase I 匿名兼容模式默认只允许 Docker CPU batch、禁用网络且不能引用 Secret：

```bash
curl -fsS -X POST http://localhost:8000/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: legacy-hello-001' \
  -d '{
    "image": "python:3.12-slim",
    "command": ["python", "-c", "print(\"hello from Mini AI Cloud\")"],
    "cpu_limit": 0.25,
    "memory_limit_mb": 128,
    "network_enabled": false
  }'
```

完整 Phase I 容器链路：

```bash
uv run python scripts/e2e_demo.py --base-url http://localhost:8000 --timeout 180
```

## 初始化项目与 API Key

只在全新数据库执行一次 bootstrap。共享环境应设置 `BOOTSTRAP_TOKEN` 并通过 `X-Bootstrap-Token` 传入；下面示例仅适用于绑定在 localhost 的一次性开发栈。

```bash
curl -fsS -X POST http://localhost:8000/api/v1/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{
    "user": {
      "username": "local-owner",
      "email": "owner@example.test",
      "password": "replace-this-local-password"
    },
    "project": {"name": "Local AI", "slug": "local-ai"},
    "api_key_name": "local-cli"
  }'
```

响应中的 `api_key.api_key` 只显示一次。不要把 key 写进 Git、日志或命令历史；CLI 省略 `--api-key` 时会隐藏输入，并尽力把本地配置权限收紧：

```bash
uv run mini-cloud auth login --url http://localhost:8000
uv run mini-cloud project list
uv run mini-cloud task list
uv run mini-cloud admin doctor
```

`mini-cloud` 是 Phase II 的短命令；兼容入口 `mini-docker-cloud` 指向同一个 CLI，Phase I 脚本无需改名。

新项目的默认镜像策略是 deny 且要求 digest。开发演示如需使用 tag，必须由 owner/admin 显式放宽到最小范围：

```bash
PROJECT_ID="<bootstrap 响应中的 project.id>"
API_KEY="<只保存到安全的本地环境或 secret store>"

curl -fsS -X PUT "http://localhost:8000/api/v1/projects/$PROJECT_ID/image-policy" \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "default_action": "deny",
    "require_digest": false,
    "rules": [{
      "action": "allow",
      "registry": "docker.io",
      "repository_glob": "library/python",
      "tag_glob": "3.12-slim",
      "priority": 100
    }]
  }'
```

然后可运行 Phase II 认证任务、timeline、usage/cost 与 artifact 真实 API 演示：

```bash
export MINI_DOCKER_CLOUD_API_KEY="$API_KEY"
uv run python scripts/phase2_demo.py --base-url http://localhost:8000
```

若再设置 `MINI_DOCKER_CLOUD_OTHER_PROJECT_API_KEY`（来自另一个 Project），脚本还会断言跨项目读取返回 404，不泄露资源是否存在。

## 架构

```text
REST / SSE / WebSocket / OpenAI-compatible API
                         |
                FastAPI + Auth/RBAC
                         |
          PostgreSQL source of truth + Outbox
                         |
              Global Scheduler / Controllers
                  /             \
      Redis events/cache       Object Store
                  \             /
              trusted multi-node Workers
                  /             \
             Docker           Kubernetes
```

PostgreSQL 保存任务、执行权、配额、reservation、服务 desired/actual state、审计与 artifact metadata。Redis 用于低延迟通知、实时事件和限流，不是任务状态真相。所有重新分配都生成新的 `execution_id`；旧执行的续租、日志和终态写回会被 fencing 拒绝。

详细设计、状态真相和故障边界见 [架构说明](docs/architecture.md)。

## 常用开发命令

```bash
make install
make format
make lint
make typecheck
make test-unit
make test-integration
make test
make config

make up
make down
make dev
make observability
make benchmark
CONFIRM_CHAOS=YES make test-chaos
make kind-up
make test-k8s
make kind-down
```

`make test-k8s` 当前验证 Kubernetes runtime 构造与 Fake GPU inventory；只有本机具备 kind/k3d 及可用镜像时才能声称真实 Kubernetes E2E。`make down` 保留卷；`docker compose down --volumes` 会永久删除当前 Compose project 的数据。

## 文档入口

- [架构与一致性边界](docs/architecture.md)
- [API 与 CLI 示例](docs/api-cli.md)
- [部署、回滚、备份恢复与排障](docs/operations.md)
- [七个强制演示](docs/demos.md)
- [Phase II 验证矩阵与缺口台账](docs/verification-matrix.md)
- [PostgreSQL hot-path 实测审计](docs/sql-review.md)

FastAPI 交互文档位于 `http://localhost:8000/docs`，OpenAPI JSON 位于 `http://localhost:8000/openapi.json`。

## 安全边界

- API、PostgreSQL、Redis、MinIO 与 Grafana 默认应只绑定 localhost；不要直接公网暴露开发 Compose。
- API Key 数据库只存 HMAC hash 与 prefix；创建时的完整 key 只返回一次。Redis 限流不可用时默认 fail closed。
- Secret 使用配置的 AES-256-GCM key ring 加密，GET 永不返回明文。日志 redaction 只能防止 Secret 原文直接出现，无法数学上覆盖任意编码、分片或变换后的泄漏。
- Docker task 默认非 privileged、capabilities 全丢弃、`no-new-privileges`、只读 rootfs、受限 `/tmp`、PID/CPU/RAM 限制且网络关闭。允许网络只表达 `none`/`internet` 的粗粒度意图，不等于域名级 egress firewall。
- Docker Worker 的 socket 权限是明确 trust boundary；Kubernetes Worker 也必须使用最小 RBAC。当前 Worker 直接连接 PostgreSQL/Redis，`WORKER_AUTH_TOKEN` 只是未来 internal worker API 的保留配置，不会认证这些直连；共享环境必须依靠独立凭据、网络 ACL，并最终引入节点 mTLS。
- 默认 Compose 给 Worker 挂载随 `COMPOSE_PROJECT_NAME` 派生的 `artifact-workspace-data` 卷，任务容器只通过 Docker `VolumeOptions.Subpath` 获取声明的单个 input/output 文件；这避免 sibling Worker 路径误映射，也避免专用 DR stack 与日常栈共享 execution workspace。裸机 Worker 与 daemon 共享宿主路径时仍使用单文件 bind。两种模式都不暴露整个 workspace；真实 Docker named-volume Subpath input→container→output E2E 已执行通过。
- Kubernetes task Pod 已固定非 root UID/GID 65532、`RuntimeDefault` seccomp、只读 rootfs 与丢弃全部 capabilities。Artifact mount 当前仍是 pinned node 上的单文件 `hostPath(type=File)`；只有 Worker workspace 与该 node 共享同一路径时才成立。仓库已有 Pod spec/mock 测试，但尚未在 Kind 做真实文件可见性 E2E，生产多节点应采用受控 object-store/PVC/CSI 数据面。
- Artifact grant 为 presigned URL 时，客户端不得转发平台 API Key。仓库演示脚本已把对象存储请求与平台认证头隔离。

更多生产前检查、Secret key rotation 和灾备边界见 [运维手册](docs/operations.md)。

## 仓库恢复点与 Git

Phase I 稳定恢复点是：

```text
c47702b feat: build distributed Docker task platform
```

Phase II 在 `feat/mini-ai-cloud-v2` 上增量演进。不要 rebase、force reset 或覆盖该稳定提交。任务明确要求不 push；本地 commit、远端 push 和部署是三件不同的事。
