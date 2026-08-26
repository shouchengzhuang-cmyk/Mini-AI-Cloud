# Mini AI Cloud Phase II 验证报告

- 验证日期：2026-08-24
- 分支：`feat/mini-ai-cloud-v2`
- Phase I 恢复点：`c47702b`
- Phase II 实现提交：`9fb41c4`
- 环境：Windows + WSL2 Ubuntu 24.04、Python 3.12、Docker Engine 29.7.2、Docker Compose 5.5.0、PostgreSQL 16、Redis 7.4

本报告区分三类证据：本机真实执行、无硬件条件下的 fake/模拟验证、以及因外部环境或破坏性边界而未执行的项目。未执行项不计为通过。

## Implemented

- 多租户控制面：User、Project、Membership、集中式 RBAC、一次性 API Key、Project Quota、Usage Ledger 与模拟成本。
- Batch Job：保持 Phase I `/api/v1/tasks` 兼容，同时加入 workload/runtime、优先级、aging、DAG、Job Group、retry policy、error taxonomy、timeline 和 task explain。
- 调度器：事务性 CPU/RAM/GPU reservation、具体 GPU device 分配、binpack/spread、labels、taints/tolerations、DRF、公平队列、抢占计划与 execution/session fencing。
- Worker/Runtime：统一 `ComputeRuntime`，实现 Docker、Kubernetes、Fake 与 vLLM service runtime；Worker inventory 支持逐 GPU 属性。
- Model Service：desired/actual reconciliation、replica lease、健康摘除、autoscaling、fake inference 与 OpenAI-compatible gateway。
- 数据与安全：Model Registry、Artifact/Task Artifact、Dataset、AES-256-GCM Secret、流式日志脱敏、Audit、WebSocket/Event cursor、image allowlist。
- 运维：Prometheus metrics、Grafana provisioning、cleanup/retention、诊断与保守 repair、备份/恢复脚本、CI、pre-commit、CLI 和调度模拟器。

## Architecture

PostgreSQL 是 Task、execution ownership、quota/reservation、usage、service desired/actual state、audit 和 artifact metadata 的唯一状态真相。Transactional Outbox 把已提交事件投递到 Redis；Redis 只承担低延迟通知、实时事件、日志流和限流，不承担最终状态。

Global Scheduler 在数据库事务内锁定候选、Project quota 与 Worker inventory，保存具体 placement/reservation 后再发布事件。Worker 每次注册生成新的 `worker_session_id`，每次任务所有权变更生成新的 `execution_id`；旧 session/execution 的心跳、日志、artifact 和终态写回均被拒绝。Service controller 同样以 lease/generation 协调副本 reconciliation。

Runtime abstraction 允许 Docker 与 Kubernetes 共享 prepare/start/logs/wait/stop/cleanup 生命周期。当前本机实际数据面为 Docker；Kubernetes 路径已实现并做单元/spec 验证，但没有伪装成已运行过 Kind。

## Migrations

Phase II 使用 Alembic `0003` 至 `0009`：

- `0003_identity_projects`
- `0004_runtime_scheduling`
- `0005_platform_resources`
- `0006_service_quota_resources`
- `0007_detach_usage_ledger`
- `0008_worker_list_cursor`
- `0009_outbox_event_cursor`

实际验证：

- 在精确命名的临时 PostgreSQL 数据库中从 Phase I `0002_worker_reservations` 升级到 `0009`。
- 验证 legacy Task/Worker backfill：Task 获得 legacy Project、`batch_job`、Docker runtime、CPU millicores、priority 与 FIFO order；Worker 获得 session/runtime/capacity 字段。
- 验证 Worker cursor 与 Outbox event cursor index 存在。
- 共享 Compose 数据库 `alembic current` 为 `0009_outbox_event_cursor (head)`。
- `alembic check` 输出 `No new upgrade operations detected.`。
- 临时数据库在验证完成后按精确名字删除；没有对日常数据库执行 downgrade。

## Test Results

| 命令/门禁 | 实际结果 |
| --- | --- |
| `ruff format --check .` | 216 files already formatted |
| `ruff check .` | 通过 |
| `mypy .` | 209 source files，无问题 |
| `uv lock --check` | 92 packages，lock 一致 |
| `pytest -m "not slow"` | 457 passed，0 skipped，1 warning，66.23 s |
| `pytest` | 457 passed，0 skipped，1 warning，89.72 s |
| `pytest tests/e2e/test_docker_runtime.py -q` | 8 passed，含 named-volume `Subpath` input→container→output |
| 强制 Demo 3/4/5 + quota 定向组 | 28 passed |
| `make test-k8s` | 25 passed；这是 K8s runtime/Fake GPU 单元验证，不是 Kind E2E |
| Compose 默认 config | 通过 |
| Compose `artifacts + observability` profiles config | 通过 |
| PowerShell fault script AST / case list | 0 syntax errors；精确列出 10 项 |
| pre-commit 等价只读检查 | 246 files 的 large/JSON/YAML/conflict/EOF/whitespace 检查通过 |

唯一 warning 为 Starlette `TestClient` 与当前 httpx 集成的弃用提醒；不影响本轮行为测试。完整 `pre-commit run --all-files` 没有执行：项目环境未安装 pre-commit，临时安装又被 PyPI 代理超时阻断；Ruff 与所有非写入型等价检查已独立通过。

全量测试一度在两个 pytest 进程并发时发现 Docker 返回 409 `removal ... already in progress`。该清理竞态已改为窄范围幂等成功并新增回归；随后两条全量命令串行重跑均为 457 passed。

## E2E Results

| Demo | 本次证据 | 结论 |
| --- | --- | --- |
| 1 Legacy Task | 已提交快照重新 build；Task `6898a466…`，execution `7fd64394…`，Docker Worker `575774e9…`，DB logs=6、SSE logs=6、exit=0 | 本机真实 Docker E2E 通过 |
| 2 Authenticated Project Task | Project `83fa…`，Task `526f…`，execution `a931…`；跨 Project 读取返回 404；4 条日志、9 条 timeline、usage execution=1、cost 与 artifact SHA-256 校验通过 | 本机认证 API/Project 隔离/quota 读取/usage/cost/artifact E2E 通过 |
| 3 GPU Scheduling | 明确拓扑 Worker A=4×A100、Worker B=1×RTX4090；2×A100 只落 A，RTX4090 只落 B，具体 device ID 与 mismatch reason 均断言 | Fake inventory/policy 通过；无 NVIDIA 实机证据 |
| 4 Fencing | Chaos Task `65347ee8…`；old execution `a23d240c…`，new execution `131c83fc…`；旧完成被拒绝 | 本机真实 Compose lease loss/reassignment/fencing 通过 |
| 5 Preemption | 低优先级 preemptible GPU reservation → durable intent → fenced stop → 释放一次 → 高优先级任务 placement 的 PostgreSQL integration 通过 | 控制面/资源一致性通过；无真实 GPU 容器抢占 |
| 6 Service Reconciliation | Service `d9bd…` desired=2；精确终止 replica `55aa…` 的 fake process，controller 创建 replacement `5a9a…`，最终 healthy=2 | 本机真实 API + fake inference process reconciliation 通过 |
| 7 Disaster Recovery | 生成 `build/sre-backups/mini-docker-cloud-20260824T024439Z`；manifest/checksum 与 `pg_restore --list` 通过 | 只验证非破坏性 backup；未删除 volume、未 restore，不计为 DR 演练通过 |

认证 Demo 只在创建时短暂持有 API Key；输出与本报告均不包含完整 credential。

## Compose and Smoke

对提交 `9fb41c4` 执行 `docker compose up --build -d`，本地镜像以 package version `0.2.0` 构建成功。API、Worker、PostgreSQL、Redis 均为 healthy；`/readyz` 返回 PostgreSQL/Redis `ok`，`/metrics` 返回 bounded-label Prometheus 指标。随后重新执行 Legacy Docker/SSE E2E 并通过。

Prometheus/Grafana 容器的真实启动重试两次，都因 Docker daemon 配置的代理 `172.18.160.1:7891` 连接 Docker Hub 超时而无法取得镜像。不能据此声称 dashboard 已在浏览器加载；仅能确认 Compose profile、Prometheus 配置、Grafana provisioning JSON 和 metrics 行为测试通过。

## Chaos Results

在同一次完整运行中，以下 10 项全部 PASS，脚本退出码 0：

1. Redis unavailable：PostgreSQL fallback 完成任务。
2. PostgreSQL unavailable：DB-backed API 失败，health 返回 degraded/503。
3. Image pull failure：代理超时被持久化为稳定 image pull error。
4. Command exit 1：终态 failed、exit code 1。
5. Task timeout：终态 timed_out，容器删除。
6. Worker SIGKILL：lease recovery 后任务完成，`recovery_count=1`。
7. API restart：运行中任务继续成功。
8. Duplicate Redis enqueue：只接受一个 execution/container start。
9. Stale Worker result：旧 execution completion 被 fencing 拒绝。
10. Cancel running：终态 cancelled，容器删除。

首次运行还暴露了 Windows Docker CLI 缺失时，短生命周期 `wsl.exe` 调用之间 WSL 会自动关机。fault script 现在在 WSL fallback 模式创建隐藏 keepalive，并在 `finally` 精确回收；它还先 inspect 本地基础镜像，只在缺失时拉取，避免把外网可用性混入本地 timeout/cancel 场景。

未执行的扩展 chaos：MinIO restart、真实 scheduler process crash、Kubernetes node loss、慢 PostgreSQL、磁盘满与多节点网络分区。Service replica crash 与 quota 并发分别由真实 fake process 演示和 live PostgreSQL integration 覆盖，但不等于完整 chaos 矩阵。

## Benchmark Results

参数：100 workers、每节点 4 GPU、共 400 GPU、10,000 个 1/2/4-GPU jobs、seed `20260823`。结果是 deterministic in-process event-loop simulation，不包含数据库、网络或容器启动开销。

### 不启用抢占

| Policy | placements/s | utilization | p50 queue | p95 queue | p99 queue | makespan | fragmentation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| binpack | 17,442.759 | 84.266706% | 0.938210 s | 6,891.065058 s | 9,862.892423 s | 14,591.303414 s | 1.571730% |
| spread | 17,571.214 | 84.178246% | 0.990106 s | 6,954.815779 s | 10,116.220424 s | 14,606.636879 s | 2.140886% |

两者均完成 10,000 jobs。此次 seed 下 binpack 的利用率、queue tail、makespan 和 fragmentation 略优；spread 的模拟器自身 placements/s 略高。后者只是本次 Python 进程的运行速度，不是调度服务吞吐。

### 启用抢占

| Policy | placements | preemptions | placements/s | utilization | avg queue | p95 queue | makespan | fragmentation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| binpack | 15,595 | 5,595 | 494.263 | 91.644959% | 1,976.321630 s | 9,136.386208 s | 14,319.612452 s | 0.281871% |
| spread | 15,595 | 5,595 | 480.355 | 91.646144% | 1,991.553865 s | 9,237.239946 s | 14,347.015958 s | 0.630468% |

两者仍完成全部 10,000 jobs。抢占提高了模拟利用率并减少 fragmentation，但重启低优先级任务显著增加 placements 和 queue tail，不能把它解释为无条件收益。

原始本机输出位于忽略目录：

- `build/scheduler-simulation-final/scheduler-simulation.{json,csv}`
- `build/scheduler-simulation-preemption-final/scheduler-simulation.{json,csv}`

## Security Findings Fixed

- API Key 只存 HMAC hash/prefix、完整 key 只显示一次；密码使用 Argon2。
- 中央 RBAC、Project scope、last-owner 约束与跨 Project not-found 语义，修复多处潜在 IDOR/角色越权。
- AES-256-GCM Secret 按 Project/version 绑定，Worker 临时解析；日志进行 streaming best-effort 直接值脱敏。
- Artifact grant、路径规范化、checksum/size 上限和 project ownership 防止 traversal、poisoning 与跨租户读取。
- Gateway 拒绝 loopback/private/link-local 目标、禁止危险转发头/redirect，并给非 SSE 响应设置默认 16 MiB 硬上限。
- Docker task 默认 non-privileged、只读 rootfs、`no-new-privileges`、drop all caps、PID/CPU/RAM 限制、网络关闭；GPU task 必须使用 scheduler 分配的具体 device ID。
- Kubernetes Pod 使用 non-root UID/GID 65532 与 `RuntimeDefault` seccomp，并固定 labels/resource requests/limits。
- Image policy 默认 deny/digest-aware；production 禁止 Fake GPU inventory。
- Request body/upload/log limits、Redis API-key rate limit、审计与 Secret-safe error handling 已覆盖。
- Docker concurrent cleanup 的 404/409 race 变为窄范围幂等，不吞掉其他 409。

## Known Limitations

1. 本机没有 NVIDIA GPU、NVIDIA Container Toolkit、Kind/k3d 或多台物理节点；Kubernetes、真实 GPU visibility/OOM/preemption 和 vLLM 只完成生产路径实现与 fake/spec 测试。
2. Prometheus/Grafana 镜像受 Docker Hub 代理超时阻断；MinIO profile 也未做实际启动，因此没有真实 dashboard、MinIO upload/download 或 object-store restart 证据。
3. DR 只完成 backup、checksum 与 archive/restore 防御测试；精确 volume 删除/restore 仍需操作者对专用栈再次确认，不能写作已通过。
4. Worker 当前直接连接 PostgreSQL/Redis；`WORKER_AUTH_TOKEN` 是未来 internal API 预留，不保护这些直连，也不等于节点 mTLS。
5. 当前没有独立 platform-admin identity；Project admin 的全局 Worker 管理边界只适合单管理域原型。
6. 网络任务只表达 `none`/`internet`，没有细粒度 egress/metadata firewall；Docker 自定义 seccomp/AppArmor、镜像签名/扫描尚未实现。
7. OpenTelemetry 只有关键 `trace_id` propagation，没有完整 SDK/exporter trace；部分次级列表仍保留 offset。
8. 没有系统化 linearizability history checker、长时间多 scheduler soak、百万级 usage/outbox 数据或 1000/100000 模拟 profile。
9. GitHub Actions 语法和本地门禁已验证，但远端 CI 尚未运行，因为本次按要求没有 push。

## How to Run

最短本地路径：

```bash
cp .env.example .env
uv sync --all-groups
docker compose config --quiet
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/readyz
uv run python scripts/e2e_demo.py --timeout 180
```

认证 Project、image policy、CLI、Service、backup/restore 和安全边界见 [README](../README.md)、[演示手册](demos.md)、[运维手册](operations.md) 与 [架构说明](architecture.md)。

最终门禁：

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest -m "not slow"
uv run pytest
make benchmark
CONFIRM_CHAOS=YES make test-chaos
```

## Git Commits

- `c47702b feat: build distributed Docker task platform`：未修改的 Phase I 恢复点。
- `9fb41c4 feat(platform): add multi-tenant AI compute control plane`：Phase II 实现、迁移、测试与工具。
- README、架构文档与本报告位于随后一笔本地 docs commit；精确 hash 以 `git log --oneline -3` 为准。

远程 `origin` 已配置为 `https://github.com/shouchengzhuang-cmyk/mini-docker-cloud.git`。本次没有 push、没有创建 PR、没有部署到远程环境。
