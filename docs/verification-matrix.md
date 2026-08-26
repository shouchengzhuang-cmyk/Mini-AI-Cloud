# 验证证据矩阵与缺口台账

审计快照：2026-08-25。Phase IV-A.1 验收代码 head 为 `db26c3a2b1297f589f5323b87cbbe1cf9c20b766`。[GitHub Actions run 32812823700](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/actions/runs/32812823700) 的 `quality`、真实 PostgreSQL `integration` 与真实 `kind-serving-e2e` 均已通过。

## 状态含义

- **PASS**：对应测试已在注明的 commit 或验证快照中实际完成。
- **PENDING**：实现或测试已在工作区出现，但尚无当前增量的完整执行结果，不算通过。
- **NOT RUN**：没有实际执行该环境或数据面。
- **N/A**：该能力与此环境无直接关系。

Unit、manifest、mock Kubernetes client、Fake GPU 和 Fake inference 都只能证明各自那一层。它们不能替代 PostgreSQL 并发、Docker 容器、Kind 集群或真实 GPU 证据。

## 跨运行时证据

| Capability | Unit | PostgreSQL Integration | Docker E2E | Kind E2E | Real GPU |
| --- | --- | --- | --- | --- | --- |
| Batch scheduling 与 atomic claim | PASS：scheduler policy、placement、quota、reservation 与 query tests | PASS：真实 PostgreSQL atomic claim 只有一个 winner；基线 integration job 已通过 | PASS：2026-08-24 Task、日志、取消、超时与容器生命周期 | NOT RUN：batch artifact 的 Kind 文件可见性未测 | NOT RUN |
| Worker session 与 execution fencing | PASS：repository、worker session 和 stale execution tests | PASS：真实 PostgreSQL session takeover、stale mutation 与 cancellation fence tests | PASS：Compose chaos 中旧 execution completion 被拒绝 | PASS：删 Pod replacement 与 controller restart adoption 验证 execution identity | N/A |
| `FOR UPDATE`、`SKIP LOCKED` 与 controller claim | PASS：query contract 和 bounded scan tests | PASS：真实 PostgreSQL concurrent claim、session takeover、cancelled Kubernetes I/O 和 skip-locked scan | N/A | N/A | N/A |
| Service desired state 并发收敛 | PASS：reconciler、quota 与 repository tests | PASS：真实 PostgreSQL concurrent reconcilers 创建准确副本数 | N/A | PASS：2 到 4 到 1 收敛 | N/A |
| Runtime disabled 时的 Kubernetes scale admission | PASS：正向 scale fail-closed、controller lifecycle 与 scale-to-zero tests | N/A | N/A | N/A：API admission 不依赖 Kind | N/A |
| Active-request drain 与 SSE 缩容 | PASS：controller drain tests；Fake HTTP/SSE host-process integration 已通过，但不算 Docker 或 Kind | NOT RUN | NOT RUN | PASS：活跃 SSE 下 4 到 1，draining Replica 不接新请求 | N/A |
| Kubernetes Pod readiness | PASS：mock CoreV1 Pod condition 与 endpoint state mapping | N/A | N/A | PASS：2 个与 4 个真实 Pod Ready，Gateway 可访问 | N/A |
| Kubernetes Pod deletion 与 replacement | PASS：missing Pod、execution fencing 与 replacement backoff tests | N/A | N/A | PASS：手工删 Pod 后旧 execution terminal、新 Ready identity 收敛 | N/A |
| Controller restart adoption | PASS：exact workload contract、Pod UID、execution 与 session adopt tests | PASS：真实 PostgreSQL session takeover | N/A | PASS：Deployment restart 后 Pod/execution identity 不变且无重复资源 | N/A |
| Bad image bounded failure/backoff | PASS：image pull failure 与 backoff state tests | N/A | N/A | PASS：`IMAGE_PULL_FAILED`、`retry_not_before` 前不替换、有限重试 | N/A |
| Managed resource recovery isolation | PASS：合法 A/B 恢复、漂移 C quarantine、全局 API failure tests | N/A | N/A | NOT RUN：未在 Kind 人工注入 label/spec drift | N/A |
| Kubernetes serving metrics state semantics | PASS：只导出有观测依据的 bounded states | N/A | NOT RUN：未采集真实容器时序 | NOT RUN：Kind 验收未抓取 Prometheus 时序 | N/A |
| GPU gang scheduling 与具体 device reservation | PASS：Fake GPU inventory 下的单节点 tensor-parallel placement | NOT RUN：该场景未在 live PostgreSQL 重跑 | NOT RUN：Fake vLLM runtime 不算 Docker vLLM | N/A：Phase IV-A Kind 只运行 Fake inference Pod | NOT RUN |
| vLLM real inference | PASS：launch spec、controller 与 policy tests；没有真实推理证据 | N/A | NOT RUN | N/A：本阶段不在 Kind 运行 vLLM | NOT RUN |
| Task artifact input/output | PASS：workspace fencing、hash、size 与 mount contract tests | N/A | PASS：named-volume `Subpath` input 到 container 到 output | NOT RUN：pinned `hostPath(type=File)` 文件可见性未测 | N/A |

## Phase IV-A.1 验收状态

| 验收项 | 当前证据 | 状态 |
| --- | --- | --- |
| 正向 scale 在 Kubernetes runtime disabled 时 fail-closed | API regression 与 controller lifecycle tests；desired 与 pending 数不变 | PASS |
| Disabled runtime 下接受 scale-to-zero，并保留 deferred cleanup 语义 | API regression 明确断言 `stopping`、`draining` 与非伪造 cleanup | PASS |
| 单个 malformed 或 drifted managed resource 不阻断合法 recovery | A/B 恢复、C quarantine 且不 LOST/replacement/delete | PASS |
| Kubernetes API 全局失败使 recovery cycle 明确失败 | 403 与 timeout propagation tests | PASS |
| Metrics 不把未 Ready Pod 记作 ready，不导出无观测依据的状态 | bounded-state metric tests | PASS |
| PostgreSQL controller claim、session takeover 与 stale result rejection | 本机真实 PostgreSQL 4 tests；远端 integration job 133 passed | PASS |
| DB row lock 横跨 Kubernetes I/O 时，cancellation 释放锁且旧 controller 不误删 | 真实 PostgreSQL delay/cancel test 与 ADR 0001 | PASS |
| Real Kind HTTP、SSE、round-robin、replacement、2 到 4 到 1 drain、restart adoption、bad image backoff | 本机两次通过；远端 Kind job 1 passed、77.56 秒 | PASS |

## 已确认的执行记录

- 验收代码 `db26c3a2b1297f589f5323b87cbbe1cf9c20b766`：[GitHub Actions run 32812823700](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/actions/runs/32812823700) 的 `quality`、`integration` 和 `kind-serving-e2e` 均为 `SUCCESS`。
- 2026-08-25 本机：两轮完整 pytest 均为 572 passed、1 skipped、1 warning；真实 PostgreSQL targeted tests 为 4 passed。
- 2026-08-25 本机 Kind serving：两次 mandatory E2E 分别为 1 passed、55.67 秒和 1 passed、61.20 秒，均完成集群与凭据清理。
- 2026-08-24 Docker/Compose：Batch Task、artifact named-volume `Subpath` 与 lease reassignment fencing 已实际执行。详情见 [Phase II 最终验证报告](verification-report-2026-08-24.md)。

完整命令、远端 job 计数和证据边界见 [Phase IV-A.1 验证报告](verification-report-phase4a-2026-08-24.md)。

## 仍未获得的证据

1. 没有真实 NVIDIA GPU、NVIDIA Container Toolkit 或 vLLM 推理证据。Fake GPU 只验证调度逻辑，Fake inference 只验证服务生命周期和 Gateway 协议。
2. 没有跨物理节点 tensor parallel、多节点网络分区、Kubernetes node loss、HA PostgreSQL/Redis 或生产集群安全审计。
3. Batch task 的 pinned `hostPath(type=File)` 只有 spec 测试，没有 Kind 文件可见性 E2E。生产多节点数据面仍需 object store、PVC 或 CSI 方案。
4. Worker 直接连接 PostgreSQL/Redis，节点 mTLS、独立平台管理员边界、镜像签名/扫描和细粒度 egress policy 尚未实现。

## 最终验收命令

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv lock --check
uv run pytest -m "not slow"
uv run pytest
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.example --profile artifacts --profile observability config --quiet
bash -n scripts/kind_serving.sh
shellcheck scripts/kind_serving.sh
```

真实 PostgreSQL targeted concurrency tests 必须在 `LIVE_DATABASE_URL` 指向专用 PostgreSQL 时运行，并进入 GitHub `integration` job。真实 Kind 验收必须由 `kind-serving-e2e` job 创建专用 cluster，结束时收集无凭据诊断并执行清理：

```bash
make kind-serving-up
make test-kind-serving
make kind-serving-down
```

最终报告必须分别写清本地 commit、远端 push、GitHub Actions、Kind E2E、真实 GPU 和部署状态。任何一项没有实际运行，就保留 `PENDING` 或 `NOT RUN`。
