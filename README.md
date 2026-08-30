# Mini AI Cloud

*An evidence-driven experimental control plane for reliable AI workload scheduling and model serving.*

Python distribution、CLI、Compose project 与默认镜像统一使用 `mini-ai-cloud` / `mini-cloud` 身份；当前准备版本为 `0.5.0`。本分支只完成发布准备，不代表已经创建 GitHub Release 或部署生产。

Mini AI Cloud 是一个面向 AI 工作负载控制面正确性的轻量级实验平台。它重点研究在并发调度、Worker、Pod、Controller 故障和在线请求缩容时，如何依靠 PostgreSQL 这一状态真相源、lease、execution fencing 与 reconciliation，让任务和模型服务状态收敛。

项目定位是 **production-minded experimental system**。代码按生产系统会遇到的并发、故障和权限边界来设计，能力声明则以可复现证据为准。它不是 production-ready cloud、KServe replacement、AWS replacement，也不是 production-grade Kubernetes operator。

## 它在研究什么

- **Scheduling**：CPU、内存和具体 GPU device 如何在配额、公平性、优先级、污点与容忍度约束下分配。
- **Ownership and fencing**：Worker session、lease 和 `execution_id` 如何阻止旧进程继续续租、写回终态或删除新执行的资源。
- **Failure recovery**：API、Worker、Controller 或 Pod 重启和丢失后，reconciliation 如何从 PostgreSQL desired state 恢复，并隔离单个漂移资源。
- **Model serving lifecycle**：Replica 如何经过 starting、loading、running、draining，Gateway 如何避开不健康或正在排空的副本，以及活跃 SSE 请求结束后如何完成缩容。
- **Dual-vendor serving contracts**：NVIDIA 与 Huawei Ascend 的 Runtime Profile、准入、路由、fallback、circuit 与实际物理 variant 用量如何保持可审计；真实双硬件运行仍为 `REAL_HW_NOT_RUN`。

功能存在、自动化测试通过和真实外部运行是三种不同证据。逐项状态见 [验证证据矩阵](docs/verification-matrix.md)，M6 A1–A11 与堆叠 PR 处置见 [M6 发布覆盖表](docs/m6-release-coverage.md)，Phase IV-A.1 的实时验收状态见 [Kubernetes Serving 验证报告](docs/verification-report-phase4a-2026-08-24.md)。

## 五分钟 Quickstart

需要 Docker Engine、Docker Compose v2。宿主开发和脚本要求 Python 3.12 与 `uv`。Windows 若 Docker 只在 WSL 中可用，请在 WSL 终端执行。
“五分钟”指最短操作路径；首次拉取基础镜像和构建耗时取决于网络与主机，不承诺固定 wall time。

```bash
cp .env.example .env
docker compose config --quiet
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/readyz
```

浏览器打开 `http://localhost:8000/workbench`，输入 Project API Key，即可查看任务、模型服务、Worker、配额与最近用量。Workbench 是供开发和运维体验使用的同源轻量控制台，不是 production-grade 多租户管理后台。详细说明见 [Web Workbench](docs/workbench.md)。

成功时 `/readyz` 返回 PostgreSQL、Redis 与控制面依赖状态。完成体验后保留数据卷地停止：

```bash
docker compose down --remove-orphans
```

无需 Kubernetes 的正确性 Hero Scenario 可直接生成本地证据：

```bash
uv sync --frozen --all-groups
uv run mini-cloud demo fencing --output-dir build/hero/fencing
```

Controller adoption 与 active SSE drain 使用独立 Kind 集群，统一入口、证据边界和清理说明见
[Hero Scenarios](docs/hero-scenarios.md)。平台与 KServe、Kueue、Volcano、Ray Serve 的职责差异见
[诚实能力对照](docs/comparison.md)。

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

`mini-cloud` 是唯一主 CLI。兼容入口 `mini-docker-cloud` 暂时保留一个开发版本，功能不变，但会向 stderr 输出弃用提示；新脚本不要继续使用旧入口。

CLI 会优先读取 `MINI_CLOUD_*` 环境变量和 `~/.config/mini-ai-cloud/config.json`，并在一个开发版本内兼容旧的 `MINI_DOCKER_CLOUD_*` 变量与 `~/.config/mini-docker-cloud/config.json`。Compose project 默认名也已改为 `mini-ai-cloud`；Docker Compose 不会自动重命名既有 `mini-docker-cloud` 栈或 volume，迁移前请先备份数据，并用旧 project name 显式停止旧栈。

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
export MINI_CLOUD_API_KEY="$API_KEY"
uv run python scripts/phase2_demo.py --base-url http://localhost:8000
```

若再设置 `MINI_CLOUD_OTHER_PROJECT_API_KEY`（来自另一个 Project），脚本还会断言跨项目读取返回 404，不泄露资源是否存在。CLI 在本开发版本仍读取旧的 `MINI_DOCKER_CLOUD_CONFIG`、`MINI_DOCKER_CLOUD_URL` 和 `MINI_DOCKER_CLOUD_API_KEY`，但新配置必须使用 `MINI_CLOUD_*`。

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
make test-serving
make test
make config
make test-release

make up
make down
make dev
make observability
make benchmark
CONFIRM_CHAOS=YES make test-chaos
make kind-up
make test-k8s
make kind-down

make kind-serving-up
make test-kind-serving
make kind-serving-down
```

`make test-k8s` 验证 batch Kubernetes runtime 构造与 Fake GPU inventory。`kind-serving-*` 是独立的 Phase IV-A 真实 serving E2E，kubeconfig 与临时凭据保存在当前用户私有的 runtime state 目录，不切换用户默认 context；可用 `KIND_SERVING_STATE_DIR` 指定其他私有绝对路径。缺少 Docker、Kind 或 kubectl 时会明确返回 `NOT RUN` 和非零退出码。`make down` 保留卷；`docker compose down --volumes` 会永久删除当前 Compose project 的数据。

已有 Kubernetes-backed Service 在当前 API 进程无法提供对应 controller 时，正向 scale 会在修改 desired state 前 fail closed。Scale-to-zero 和 stop 仍会把 desired state 改为 0，但 Service 保持 `stopping`，Replica 保持 `draining`，不会伪装成 Kubernetes 资源已经删除。Controller 重新启用后从 PostgreSQL 和 managed resource labels 恢复 reconciliation，再完成 fenced cleanup；Controller 一直关闭时，资源不会自动清理。

Phase III 的 Fake Serving E2E 会真实启动 HTTP inference 子进程，并经 Gateway 验证 non-streaming、SSE、RR、failure replacement、draining、Project API Key 隔离和 TP gang placement。要同时执行 live PostgreSQL 并发用例：

```bash
LIVE_DATABASE_URL=postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform \
  make test-serving
```

完整说明与真实 vLLM 前置条件见 [Phase III AI Model Serving](docs/phase3-ai-serving.md)。
Kubernetes Pod、readiness、drain、恢复与 Kind 命令见 [Phase IV-A Kubernetes 原生模型服务](docs/phase4-kubernetes-serving.md)。

## 文档入口

- [架构与一致性边界](docs/architecture.md)
- [API 与 CLI 示例](docs/api-cli.md)
- [部署、回滚、备份恢复与排障](docs/operations.md)
- [七个强制演示](docs/demos.md)
- [验证证据矩阵与缺口台账](docs/verification-matrix.md)
- [Phase III AI Model Serving](docs/phase3-ai-serving.md)
- [Phase IV-A Kubernetes 原生模型服务](docs/phase4-kubernetes-serving.md)
- [Phase IV-A 验证报告](docs/verification-report-phase4a-2026-08-24.md)
- [PostgreSQL hot-path 实测审计](docs/sql-review.md)
- [证据与能力声明规范](docs/claim-policy.md)
- [机器可读 claims、invariants 与 environments 合同](evidence/README.md)
- [统一 Hero Scenario 运行与证据说明](docs/hero-scenarios.md)
- [版本、发布与弃用策略](docs/release-policy.md)
- [建议的 GitHub 仓库元数据](docs/repository-metadata.md)

FastAPI 交互文档位于 `http://localhost:8000/docs`，OpenAPI JSON 位于 `http://localhost:8000/openapi.json`。

## 参与与发布

提交改动前请阅读 [贡献指南](CONTRIBUTING.md) 与 [安全策略](SECURITY.md)。版本变化记录在 [CHANGELOG](CHANGELOG.md)，优先级与证据边界见 [ROADMAP](ROADMAP.md)；仓库采用 [MIT License](LICENSE)。PR 必须明确变更、非目标、验证、风险和 `REAL` / `SIMULATED` / `NOT RUN` 证据，不把未执行或模拟结果写成真实环境通过。

## 安全边界

- API、PostgreSQL、Redis、MinIO 与 Grafana 默认应只绑定 localhost；不要直接公网暴露开发 Compose。
- API Key 数据库只存 HMAC hash 与 prefix；创建时的完整 key 只返回一次。Redis 限流不可用时默认 fail closed。
- Secret 使用配置的 AES-256-GCM key ring 加密，GET 永不返回明文。日志 redaction 只能防止 Secret 原文直接出现，无法数学上覆盖任意编码、分片或变换后的泄漏。
- Docker task 默认非 privileged、capabilities 全丢弃、`no-new-privileges`、只读 rootfs、受限 `/tmp`、PID/CPU/RAM 限制且网络关闭。允许网络只表达 `none`/`internet` 的粗粒度意图，不等于域名级 egress firewall。
- Docker Worker 的 socket 权限是明确 trust boundary；Kubernetes Worker 也必须使用最小 RBAC。当前 Worker 直接连接 PostgreSQL/Redis，`WORKER_AUTH_TOKEN` 只是未来 internal worker API 的保留配置，不会认证这些直连；共享环境必须依靠独立凭据、网络 ACL，并最终引入节点 mTLS。
- 默认 Compose 给 Worker 挂载随 `COMPOSE_PROJECT_NAME` 派生的 `artifact-workspace-data` 卷，任务容器只通过 Docker `VolumeOptions.Subpath` 获取声明的单个 input/output 文件；这避免 sibling Worker 路径误映射，也避免专用 DR stack 与日常栈共享 execution workspace。裸机 Worker 与 daemon 共享宿主路径时仍使用单文件 bind。两种模式都不暴露整个 workspace；真实 Docker named-volume Subpath input→container→output E2E 已执行通过。
- Kubernetes task Pod 已固定非 root UID/GID 65532、`RuntimeDefault` seccomp、只读 rootfs 与丢弃全部 capabilities。Artifact mount 当前仍是 pinned node 上的单文件 `hostPath(type=File)`；只有 Worker workspace 与该 node 共享同一路径时才成立。仓库已有 Pod spec/mock 测试，但尚未在 Kind 做真实文件可见性 E2E，生产多节点应采用受控 object-store/PVC/CSI 数据面。
- Kubernetes serving Pod 使用非 root UID/GID 10001、只读 rootfs、受限 `/tmp`、drop ALL、`RuntimeDefault` seccomp、requests=limits，并关闭 token automount 和 host namespace。它不会挂载 Docker socket、kubeconfig 或 `hostPath`。Controller 的 namespace RBAC 仍是高权限基础设施边界，生产环境应使用独立 namespace 和 ServiceAccount。
- Artifact grant 为 presigned URL 时，客户端不得转发平台 API Key。仓库演示脚本已把对象存储请求与平台认证头隔离。

更多生产前检查、Secret key rotation 和灾备边界见 [运维手册](docs/operations.md)。

## 仓库恢复点与 Git

Phase I 稳定恢复点是：

```text
c47702b feat: build distributed Docker task platform
```

Phase II 在 `feat/mini-ai-cloud-v2` 上增量演进，Phase III 位于 `feat/ai-serving-v3`，Phase IV-A 位于 `feat/k8s-serving-v4a`。不要 rebase、force reset 或覆盖稳定提交。本地 commit、远端 push、PR 和部署是四件不同的事，验证报告会分别记录。
