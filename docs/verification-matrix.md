# Phase II 验证矩阵与缺口台账

审计快照：2026-08-24，分支 `feat/mini-ai-cloud-v2`。状态只表达当前仓库的代码/自动化证据；本次机器上的最终执行记录单独写入验证报告。

## 状态含义

- **自动化证据**：实现存在，且有行为测试；仍需最终全量执行结果。
- **外部验证待做**：生产路径存在，但当前证据依赖 Docker/GPU/Kubernetes/破坏性专用栈。
- **部分完成**：核心路径存在，mission 的某个硬性边界仍缺。
- **缺口**：没有发现满足要求的可执行实现或证据，不能在最终报告写 implemented。

## 功能覆盖

| Mission 范围 | 当前状态 | 仓库证据 | 尚需验证/补齐 |
| --- | --- | --- | --- |
| 0-4 Git 安全、增量演进 | 自动化证据 | Phase I 恢复点 `c47702b`；Phase II 独立分支 | 最终 commits 与 clean status；禁止 push |
| 5 Runtime abstraction | 自动化证据 | `worker/runtime.py`, registry, Docker/Fake/K8s tests | 全量 regression |
| 6-7 Kubernetes/Kind | 外部验证待做 | `worker/kubernetes_runtime.py`, `tests/unit/test_kubernetes_runtime.py`, Make targets | 真实 kind API→Pod→logs→terminal E2E；context 恢复 |
| 8-9 Worker inventory/reservation | 自动化证据 | Worker v2 schema、ResourceReservation/GPUDevice、lease/reaper tests | 多物理节点、网络分区实测 |
| 10-12 GPU allocation/Fake GPU | 自动化证据 | per-device inventory、production fake guard、Docker 只接受具体 device ID；pull/global claim 都拒绝无设备绑定的 GPU reservation | 真实 NVIDIA visibility/OOM/cleanup |
| 13-17 Scheduler v2/admission | 自动化证据 | binpack/spread、priority/aging、DRF、taint/toleration、preemption、unschedulable reason 与 scheduling explain API | 真实 multi-scheduler load |
| 18-21 Users/Auth/Projects/RBAC | 自动化证据 | Argon2, HMAC API keys, central permission map, identity integration tests | 共享部署 credential rotation runbook 实测 |
| 22-24 Quota/Usage/Cost | 自动化证据 | row locks、non-negative invariants、immutable ledger、usage APIs | 高并发 PostgreSQL quota E2E 与价格配置审查 |
| 25-34 Registry/Service/vLLM/Gateway | 自动化证据 + 外部验证待做 | registry、service reconciliation/lease/health/autoscaler、fake inference；opt-in Docker vLLM fenced controller；gateway SSRF/header/redirect/response-size tests | 真实 vLLM/GPU；scale-to-zero cold-start API 行为复验 |
| 35 Image policy | 自动化证据 | canonical reference、deny-by-default、digest/rules、task admission | resolved runtime image digest 端到端展示仍需确认 |
| 36-37 Secret/redaction | 自动化证据 | AES-256-GCM key ring、project binding、pinned version、streaming redaction tests | KMS integration；任意编码泄漏明确不承诺 |
| 38-40 Audit/events/WebSocket | 自动化证据 | append-only audit repository/API、cursor、project-scoped WS tests | 跨多 API replica 的实时 fan-out/背压实测 |
| 41 Distributed scheduler safety | 自动化证据 | row lock/`SKIP LOCKED`、atomic claim/placement tests | 两个真实 scheduler 进程并发 soak |
| 42 Benchmark | 自动化证据 | deterministic simulator，10k scenario CLI，JSON/CSV | 将本次实际 p50/p95/p99/placements/sec 记入最终报告 |
| 43-44 Explain/placement attempts | 自动化证据 | placement attempts 与 unschedulable reason 持久化；`GET /api/v1/tasks/{id}/scheduling` 聚合 considered/rejections；CLI `task explain` | 大量历史 placement attempt 的分页/retention 实测 |
| 45-49 labels/taints/drain/re-register/protocol | 部分完成 | taint/toleration、admin drain、worker session fencing、re-register/reservation tests；当前 Worker 直接使用 DB/Redis，没有伪装成 internal API | 节点 mTLS 仅保留设计边界；`WORKER_AUTH_TOKEN` 是未来 internal API 预留，当前不保护 DB/Redis 直连 |
| 50 Object storage | 自动化证据 | Local/S3 abstraction、presigned URL、size/hash/path tests | MinIO profile 真实 upload/download E2E |
| 51-53 Task input/output artifacts | 自动化证据 + Docker E2E | Task schema/bindings、fenced workspace、input hash/size、executor 顺序；Compose named-volume `Subpath` 已真实验证 input→container→output；裸机 bind 与 pinned K8s `hostPath(type=File)` 有单测 | K8s 目前仅 mock Pod spec，Kind 文件可见性与多节点数据面未测 |
| 54 Dataset | 自动化证据 | project-scoped Dataset/version API 与 referenced artifact protection tests | 大版本列表/retention integration |
| 55-57 DAG/Job Groups | 自动化证据 | cycle detection、same-project、failure policy、scheduler re-check | 大图并发/性能与 isolated-node group membership 边界 |
| 58 Retry policy | 自动化证据 | Task `RetryPolicy` 支持含首试的 `max_attempts`、fixed/linear/exponential、base/max、exit-code allowlist；旧 `max_retries` 自动映射，属性/集成测试覆盖 | 真实故障下的 backoff 时间与尝试次数 E2E |
| 59-60 Error taxonomy/OOM | 自动化证据 + 外部验证待做 | 稳定 `error_category/error_code`、每次 execution 落库、分类重试；Docker/K8s OOM/137、image pull/start/GPU 单测 | Docker 与 Kind 中真实制造 OOM、观察终态/重试/cleanup |
| 61 Task timeline | 自动化证据 | persisted TaskEvent 与 `/timeline` | live Docker recovery timeline E2E |
| 62-63 Metrics/Grafana | 自动化证据 | bounded Prometheus metrics、outbox pending/age、provisioned dashboard（含 success rate） | 实际 dashboard query/screenshot 与 alert rules |
| 64 OpenTelemetry | 部分完成 | outbox/event `trace_id` propagation | 无完整 OTel SDK/exporter/API→scheduler→worker trace |
| 65 SQL query review | 部分完成 | [SQL 实测审计](sql-review.md) 覆盖 6 条 hot path 与 buffers；scheduler/DAG batching；JobGroup 3 组列表从 11 降至固定 4 SELECT；Outbox cursor index | 小数据、零 queued/edges/usage，需生产风格 benchmark；Project event 归属 OR 谓词仍复杂 |
| 66-68 Migrations/compat/versioning | 自动化证据 | Alembic 0003-0009、`/api/v1` 兼容、legacy tests；专用 PostgreSQL 已从 Phase I `0002` 升级并检查 backfill | 最终完整 gate 仍需记录；破坏性 downgrade 只允许专用测试库 |
| 69 OpenAPI | 自动化证据 | schema generation、核心路径/schema、operation id 唯一性、Bearer/X-API-Key/bootstrap security schemes 与公共 error response | descriptions/examples 尚未覆盖每个次要字段与 endpoint |
| 70 Pagination | 部分完成 | audit/events/Task/Worker/Service/Artifact 使用不透明 cursor；offset 仅为兼容路径 | Project/Dataset/Registry/JobGroup/Replica 等次级列表仍是 offset |
| 71-72 Rate/body/response limits | 自动化证据 | API key Redis minute limit、fail-closed option、chunked/body middleware；Gateway 非 SSE 16 MiB 默认硬上限 | 多 replica rate semantics 与 Redis outage live test |
| 73-77 Runtime/network/SSRF/supply chain | 部分完成 | Docker caps/rootfs/tmpfs/network limits；K8s non-root/RuntimeDefault seccomp；gateway SSRF/header defense；image policy | Docker 自定义 seccomp/AppArmor、细粒度 egress/NetworkPolicy、签名/扫描、resolved digest persistence 不完整 |
| 78-79 Cleanup/retention | 自动化证据 | CleanupController、DB/Redis/artifact cleanup、expired API key、reference protection；Task 删除后 UsageLedger 通过不可变标识保留 | orphan Pod 与大数据量小批次 soak 仍需验证 |
| 80 Backup/restore | 部分实测 | guarded scripts、checksum、archive traversal/link defense；本机 backup manifest/checksum 与 `pg_restore --list` 通过 | 专用 Compose destructive restore 仍未执行 |
| 81 Disaster recovery | 外部验证待做 | Demo 7 安全 runbook | 未执行精确 volume destruction/restore 不能写通过 |
| 82 Chaos expansion | 部分完成 + 本机实测 | 10 个 fault cases 已完整单次运行通过，包含 Redis/DB/worker/API/fencing/cancel；service replica 与 quota 另有实测 | 慢 SQL、object store、scheduler crash、K8s node loss 场景不足 |
| 83 Race/linearizability | 部分完成 | claim/fencing/quota/preemption/service lock tests | 系统化 history checker/long soak 未发现 |
| 84 Property-based tests | 自动化证据 | Hypothesis 覆盖终态吸收、scheduler 容量、quota 非负、DAG cycle 与 retry backoff 上界 | 尚未把 property test 扩展为数据库并发状态机/history checker |
| 85-86 Invariants/DB constraints | 自动化证据 | checks/uniques/FKs、non-negative tests、`alembic check` workflow | PostgreSQL constraint/race full suite 最终重跑 |
| 87-90 Static/CI/pre-commit | 自动化证据 | Ruff, strict mypy, pytest, CI, pre-commit | 最终所有 gate 输出；CI 尚未在远端运行 |
| 91 Developer UX | 自动化证据 | Make targets、分层文档 | `make test-k8s` 名称可能被误解为真实 K8s E2E，文档已明确边界 |
| 92-94 CLI/explain/admin | 自动化证据 | `mini-cloud` 与兼容 `mini-docker-cloud` 指向同一 Typer app；auth/project/task/service/usage/doctor；task explain 与 scheduler/admin diagnostics API | worker 管理命令较薄；缺独立 platform-admin 身份边界 |
| 95 Consistency checker | 自动化证据 | project-scoped diagnostics、RBAC、CLI `--repair`；只幂等修 terminal reservation/lease，processed outbox 与负容量检查 | 跨节点 runtime inventory 不可观测，orphan container/pod 明确返回 `not_observable` 且不自动修 |
| 96-100 Simulator/fair Scheduler | 自动化证据 | deterministic simulator、fragmentation、binpack/spread、DRF | 1000/100000 scale profile 与真实 DB scheduler comparison |
| 101-105 独立 reviews | 部分完成 | 安全/架构/SRE/cleanup 审计与定向修复；SQL review 有本机 `EXPLAIN (ANALYZE, BUFFERS)` 记录 | 数据规模小且分布单一，仍缺生产风格 scheduler/log/event profile；硬性缺口不能用文档代替 |
| 106 完整验证 | 本机可执行项已完成 | [最终验证报告](verification-report-2026-08-24.md) 记录静态门禁、457 tests、Compose build/health、Docker E2E、chaos 与 benchmark | 外部 GPU/K8s/MinIO/observability/DR 限制单列，不计为通过 |
| 107-113 七个 Demo | 部分完成 + 分项实测 | [演示手册](demos.md)；Demo 1/2/4/6 本机实测，Demo 3/5 为 fake/integration，Demo 7 仅 backup | 真实 GPU/K8s 与 destructive DR 仍受环境/确认边界限制 |
| 114-120 tests/commits/report | 当前环境已完成 | 457 行为测试、两笔逻辑本地 commits、最终报告；Phase I 恢复点保留 | 明确未 push，远端 CI 尚未运行 |

## 当前硬性缺口（不能被文档掩盖）

以下项目仍不满足 mission 的“code + migration + tests + integration”完成门槛：

1. Worker 目前直接连接 DB/Redis；没有节点 mTLS，也没有由 `WORKER_AUTH_TOKEN` 认证的 internal worker API。session/execution fencing 能防旧会话写入，但不等于节点身份认证。
2. Project/Dataset/Registry/JobGroup/Replica 等次级列表尚未统一 cursor；OpenAPI descriptions/examples 也未覆盖全部次级接口。
3. OpenTelemetry 完整 trace，以及在有 queued/dependency/usage 的生产风格数据上做 SQL/profile；当前已消除 scheduler/DAG/JobGroup 的已知 N+1。
4. 系统化数据库并发状态机与 linearizability history checker；现有 Hypothesis 覆盖纯函数/invariant properties，但不等于长时间并发证明。
5. Docker 自定义 seccomp/AppArmor、允许网络任务的细粒度 egress/metadata 防护、镜像签名/扫描等生产安全层。
6. 独立 platform-admin 身份边界；当前 Project admin 仍可查看/排空全局 Worker，适合单管理域原型，不适合不互信租户。
7. 真实 kind（含 pinned file 可见性）、NVIDIA/vLLM、多物理节点、MinIO、真实 OOM、完整 chaos 与破坏性 DR 演练。Docker named-volume Subpath E2E 已通过，不再列为缺口。

这些缺口应在最终报告 `Known Limitations` 中保留，除非后续确实实现并验证。

## 最终命令矩阵

### 静态与自动化

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest -m "not slow"
uv run pytest
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.example --profile artifacts --profile observability config --quiet
```

### 数据库 migration

```bash
export DATABASE_URL='postgresql+asyncpg://task:local-dev-only@localhost:5432/task_platform'
uv run alembic downgrade 0002_worker_reservations  # 只在专用测试数据库
uv run alembic upgrade head
uv run alembic check
```

### Compose smoke/E2E

```bash
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/readyz
uv run python scripts/e2e_demo.py --timeout 180
uv run python scripts/phase2_demo.py --timeout 180
make benchmark
```

### 环境条件成立才运行

```bash
make test-docker
CONFIRM_CHAOS=YES make test-chaos
make kind-up
make test-k8s
make kind-down
make backup
make restore BACKUP=/absolute/path CONFIRM_RESTORE=YES
```

## 最终报告模板

最终报告至少分开：

1. **Implemented**：只列代码与 migration 已存在的能力。
2. **Architecture**：PostgreSQL truth、Redis role、fencing、quota/reservation、service reconciliation。
3. **Migrations**：起点、终点、真实命令、backfill 检查、`alembic check`。
4. **Test Results**：命令、pass/fail/skip 数和失败修复，不写估计数。
5. **E2E Results**：七个 Demo 分项，区分 fake 与真实 external runtime。
6. **Chaos Results**：实际执行 case 与未执行 case。
7. **Benchmark Results**：参数、机器、真实 JSON/CSV 指标，不预设策略结论。
8. **Security Findings Fixed**：具体问题、修复边界和剩余 trust boundary。
9. **Known Limitations**：保留上面的硬性缺口与环境限制。
10. **How to Run**：README/docs 的最短命令。
11. **Git Commits**：本地 commit hashes；明确 `not pushed`。

“implemented but environment prevented real-world verification”必须与“actually tested”分栏，不能互相替代。
