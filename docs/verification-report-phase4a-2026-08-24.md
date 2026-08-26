# Phase IV-A.1 验证报告

更新日期：2026-08-25。

本报告跟踪 PR [#1 feat: add Kubernetes-native model serving](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/pull/1)。目标分支为 `feat/k8s-serving-v4a`，基线为 `feat/ai-serving-v3`。Phase IV-A.1 验收代码 head 为 `db26c3a2b1297f589f5323b87cbbe1cf9c20b766`，PR 保持 `OPEN`、未 merge。

报告只把实际完成的命令写成 `PASS`。实现、测试文件和 CI job 已存在，但还没有当前增量的执行结果时，一律写成 `PENDING` 或 `NOT RUN`。

## 当前验收证据

### 本机质量门禁

以下结果来自 2026-08-25 的 Ubuntu 24.04 WSL2 本机工作区，代码与 `db26c3a2b1297f589f5323b87cbbe1cf9c20b766` 一致，文档改动除外：

| 命令 | 结果 |
| --- | --- |
| `uv run ruff format --check .` | PASS，234 个文件已格式化 |
| `uv run ruff check .` | PASS |
| `uv run mypy .` | PASS，222 个源文件无问题 |
| `uv lock --check` | PASS，解析 92 个包 |
| `uv run pytest -m "not slow"` | PASS，572 passed、1 skipped、1 warning，91.52 秒 |
| `uv run pytest` | PASS，572 passed、1 skipped、1 warning，92.41 秒 |

唯一 warning 来自 FastAPI TestClient 依赖链中的 Starlette deprecation warning。唯一 skip 是通用 pytest 中需要 `KIND_SERVING_E2E=1` 的专用 Kind 入口；真实 Kind 通过 Make 入口单独执行并通过。真实 PostgreSQL/Redis tests 在本机 Compose 数据层上实际运行，没有把 live backend skip 计为通过。

### 远端 CI

[GitHub Actions run 32812823700](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/actions/runs/32812823700) 针对 `db26c3a2b1297f589f5323b87cbbe1cf9c20b766` 完整执行并通过：

- `quality`：PASS，431 passed、142 deselected、1 warning。
- `integration`：PASS，133 passed、440 deselected、1 warning；包括真实 PostgreSQL controller concurrency/fencing tests。
- `kind-serving-e2e`：PASS，1 passed，77.56 秒；Kind cluster 创建、mandatory E2E、credential-safe diagnostics 和 always cleanup 均成功。

前一轮 [GitHub Actions run 32812209380](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/actions/runs/32812209380) 的 `quality` 与 `integration` 已通过，但 Kind E2E 暴露了删 Pod 后读取旧 DB Ready 快照的验收脚本竞态。`db26c3a` 增加 replacement identity convergence 等待；本机真实 Kind 复测 1 passed、61.20 秒，随后上述远端 run 全绿。

### 配置、部署资产与容器权限 smoke

- 默认 Compose 配置以及 artifacts、observability profiles 的 Compose 配置通过 `config --quiet`。
- `bash -n scripts/kind_serving.sh`、`shellcheck scripts/kind_serving.sh` 和 `git diff --check` 通过。
- workflow 与 `deploy/kind-serving/` 中 5 个 YAML 文件均可解析；部署目录共 13 个 YAML documents。
- 本机固定 Kind `v0.27.0`、kubectl `v1.32.2` 创建真实 cluster，mandatory E2E 1 passed、55.67 秒；修复 CI 竞态后再次 1 passed、61.20 秒，两次均完成 cluster 和私有 runtime state 清理。
- 真实 `postgres:16-alpine` 镜像在模拟 Kubernetes `fsGroup` 权限的 tmpfs 中完成 `initdb`。这只证明 non-root `PGDATA` 子目录权限修复可工作，不等于 Kind 部署通过。

## Phase IV-A.1 增量状态

| 项目 | 验收证据 | 状态 |
| --- | --- | --- |
| Review P2：runtime unavailable 时拒绝正向 scale | API 在持有 Service row lock、修改 desired state 之前检查 feature opt-in、环境和 controller lifecycle readiness；API regression 与 controller lifecycle tests 通过 | PASS |
| Disabled runtime 下 scale-to-zero | desired state 接受为 0，Service 保持 `stopping`，Replica 保持 `draining`；没有 controller 时不伪造资源已删除或 Service 已停止 | PASS |
| Recovery isolation | 单个 drifted managed resource 被 quarantine，不触发 LOST/replacement/delete；合法资源继续恢复，全局 Kubernetes API failure 继续向 cycle 传播 | PASS |
| Metrics semantics | Pod gauge 只保留 `unknown`、`not_ready`、`ready`、`terminating`；Ready 前 failure 与实际 replacement 用 counter | PASS |
| PostgreSQL controller fencing | 真实 PostgreSQL concurrent claim、session takeover、stale result rejection 和 `SKIP LOCKED` tests 在本机与 CI 通过 | PASS |
| DB lock 与 Kubernetes I/O cancellation | 真实 PostgreSQL test 验证阻塞 cleanup 期间 takeover 等锁，取消后锁释放且旧 session 不能删除 | PASS |
| Real Kind CI | 固定 Kind `v0.27.0` 与 kubectl `v1.32.2`；真实 HTTP/SSE/RR、replacement、drain、restart adoption、backoff 和 cleanup 全部通过 | PASS |

### Scale-to-zero 的 deferred cleanup 语义

Kubernetes runtime 当前不可用时，正向 scale 会 fail closed，不增加 `desired_replicas`，也不创建新的 pending Replica。Scale-to-zero 和 stop 仍可把 desired state 改为 0，已有 Replica 进入 `draining`，Service 保持 `stopping`。

API 进程不会在 controller 关闭时直接访问 Kubernetes，也不会把 Pod 删除和 Replica 终态写成已经完成。Controller 恢复后会从 PostgreSQL desired state 和 managed resource labels 重新开始 reconciliation，等待活跃请求结束或 drain deadline 到期，再执行 fenced cleanup。Controller 一直关闭时，Kubernetes 资源不会自动消失，这属于明确保留的运维边界。

## SIMULATED / UNIT ONLY

以下能力在远端基线有自动化证据，但 Kubernetes CoreV1 使用 mock client，不能算真实集群验证：

- Pod 与 ClusterIP Service 生成、DNS-1123 有界名称、稳定 labels 和 execution/generation/session fencing。
- `runAsNonRoot`、只读 rootfs、受限 memory `emptyDir`、丢弃 capabilities、`RuntimeDefault` seccomp、禁用 ServiceAccount token 和 host namespace。
- Pod Ready condition 与 Replica health 转换，以及 image pull、OOM、missing Pod 和 startup timeout 状态映射。
- graceful delete、force cleanup、UID precondition、重复删除和 orphan discovery。
- 409 冲突及 controller restart 的 exact adoption，包括 image、command/args、env、resources、port、readiness probe、安全上下文、termination grace 和 Service endpoint contract。
- SQLite repository/controller integration 覆盖 readiness、active-request drain、session takeover、restart adopt、missing Pod replacement、image pull failure 和 OOM persistence。
- Fake inference host process 覆盖 Gateway non-streaming、SSE、round-robin、health replacement 和 drain；它不是 Docker vLLM，也不是 Kubernetes Pod。

## REAL KIND E2E：PASS

本机和 GitHub-hosted Ubuntu runner 都实际创建了 Kind cluster、导入单平台应用/PostgreSQL/Redis 镜像、执行 migration，并通过 Kubernetes API 部署 API/controller 与 Fake inference Pods。远端证据为 [run 32812823700](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/actions/runs/32812823700) 的 `kind-serving-e2e` job。

实际通过的 mandatory scenarios：

- Service desired=2 后有 2 个真实 Pod Ready，并经 Gateway 完成 `/v1/models`、non-streaming、SSE 与 round-robin。
- 手工删除 serving Pod 后，旧 execution 终止并被 fencing，replacement 创建，Gateway 收敛到新 Ready identity 集合。
- 2 到 4 扩容后有 4 个 Pod Ready 和 4 个 per-Replica ClusterIP Services。
- 活跃 SSE 请求期间从 4 缩到 1，draining Replica 不接收新请求，请求结束后旧 Pod 删除，最终只保留预期健康副本。
- 重启 controller Deployment 后 adopt 原 Pod/execution，不重复创建 Replica、Pod 或 per-Replica Service。
- 坏镜像持久化 bounded `IMAGE_PULL_FAILED`，在 `retry_not_before` 前不替换，之后有限重试且不进入 hot crash loop。

CI 的 diagnostic step 在成功和失败时都会输出版本、Pods、Services、events、describe、API/controller logs 与异常 serving Pod logs，并按本地私有值做 redaction。最终 cleanup step 在本次成功 run 中实际删除 cluster 和临时凭据。

## 其他外部边界

- 没有真实 NVIDIA GPU、NVIDIA Container Toolkit 或 vLLM inference 验证。Phase IV-A 的 Kind 场景只运行 development/test Fake inference Pod。
- 没有跨物理节点 tensor parallel、多节点网络分区、Kubernetes node loss、HA PostgreSQL/Redis、公网部署或生产 Kubernetes 安全审计。
- Kind manifest 面向本地验收，不是高可用或公网生产部署方案。
- Worker 仍是可信基础设施。Docker socket、Kubernetes namespace RBAC 和 Worker 直连数据层都是明确的权限边界。

## 当前结论

Phase IV-A.1 的 admission、recovery isolation、metrics semantics、PostgreSQL fencing、DB-lock/Kubernetes-I/O trade-off 和 Real Kind mandatory scenarios 均已有本机与 GitHub Actions 证据。PR 保持 `OPEN`、未 merge，真实 GPU/vLLM、多物理节点和生产 HA/安全边界仍明确不在本阶段验收范围。

**Phase IV-A.1 implementation, hardening, PostgreSQL acceptance, and Real Kind acceptance are PASS.**
