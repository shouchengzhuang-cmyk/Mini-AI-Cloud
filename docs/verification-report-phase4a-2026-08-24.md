# Phase IV-A 验证报告（2026-08-24）

本报告记录 `feat/k8s-serving-v4a` 相对 `feat/ai-serving-v3`（基线 `61cc0c5`）的本机验证。执行环境为 Ubuntu 24.04 WSL2、Python 3.12.3；Docker Engine 可访问。结论只覆盖下列实际命令和测试，不把 manifest、mock client 或 Fake inference 写成真实 Kubernetes 生产验证。

## PASS

### 规定质量门禁

| 命令 | 结果 |
| --- | --- |
| `uv run ruff format --check .` | PASS，231 个文件已格式化 |
| `uv run ruff check .` | PASS |
| `uv run mypy .` | PASS，221 个源文件无问题 |
| `uv lock --check` | PASS，解析 92 个包 |
| `uv run pytest -m "not slow"` | PASS，545 passed、6 skipped、1 warning，124.24 秒 |
| `uv run pytest` | PASS，545 passed、6 skipped、1 warning，119.55 秒 |

唯一 warning 来自 FastAPI TestClient 依赖链中的 Starlette deprecation warning，不是本阶段新增失败。6 个 skip 包括 1 个明确标记 `NOT RUN` 的 Kind E2E，以及依赖外部 live PostgreSQL/Redis 的既有测试；没有把 skip 计为通过。

### 配置、部署资产与权限 smoke

- 默认 Compose 配置以及启用 artifacts、observability profiles 的 Compose 配置均通过 `config --quiet`。
- `bash -n scripts/kind_serving.sh`、`shellcheck scripts/kind_serving.sh` 和 `git diff --check` 均通过。
- `deploy/kind-serving/` 中 5 个 YAML 文件均能解析，共 13 个 YAML documents。
- 使用真实 `postgres:16-alpine` 镜像、UID/GID 70 和模拟 Kubernetes `fsGroup` 权限的 tmpfs，针对 `/var/lib/postgresql/data/pgdata` 实际执行 `initdb` 成功。这个 smoke 证明 non-root `PGDATA` 子目录权限修复可工作，但不等同于 Kind 部署通过。

## SIMULATED / UNIT ONLY

以下能力有自动化证据，但 Kubernetes CoreV1 调用使用 mock client，不能算真实集群验证：

- Pod 与 ClusterIP Service 的生成、DNS-1123 有界名称、稳定 labels 和 execution/generation/session fencing。
- `runAsNonRoot`、只读 rootfs、受限 memory `emptyDir`、丢弃 capabilities、`RuntimeDefault` seccomp、无 ServiceAccount token 和无 host namespace。
- Pod Ready condition 与 `running + healthy` 的转换，以及 image pull、OOM、missing Pod、startup timeout 的状态映射。
- graceful delete、force cleanup、UID precondition、重复删除和 orphan discovery。
- 409 冲突及 controller restart 时的精确 adopt：runtime 会重算 workload contract hash，并核对 image、command/args、env、resources、port、readiness probe、安全上下文、termination grace 和 volume；Service 端口必须与 Pod 一致。
- SQLite repository/controller integration 覆盖 readiness、active-request drain、Kubernetes 专用 drain timeout、session 接管、重启 adopt、Pod 丢失 backoff、image pull 和 OOM 持久化。
- 现有 Gateway、Service Reconciler 和 repository 测试仍覆盖 endpoint 选择、unhealthy/draining 排除和 replacement 数据路径；Kind 场景没有测试专用 Gateway 旁路。

`scripts/kind_serving_e2e.py` 已实现真实 API、kubectl 和 Gateway 场景，包括 2→4→1、SSE drain、手工删 Pod、controller rollout restart、坏镜像 backoff 和资源清理。脚本存在不代表这些断言已在本机执行。

## NOT RUN

### 真实 Kind E2E

执行：

```text
bash scripts/kind_serving.sh test
NOT RUN: required commands are unavailable: kind kubectl
```

入口返回非零。本机 Docker Engine 可访问，但 WSL 中没有 `kind` 和 `kubectl`，因此没有创建 `mini-ai-cloud-serving-v4a` 集群，也没有实际运行以下 mandatory assertions：

- 在 Kubernetes 中创建 Fake inference Pod，并观察 loading 到 Ready。
- 经真实 Gateway 完成 `/v1/models`、non-streaming、SSE 和 round-robin。
- 2→4→1 扩缩容、active SSE drain 和最终数据库/Kubernetes 资源收敛。
- 手工删除 Pod 后 replacement 与旧 execution fencing。
- controller rollout restart 后保持 Pod UID/execution、不重复创建 Replica。
- 坏镜像 loading failure、bounded error 和 replacement backoff。

脚本没有静默 skip，也没有安装或修改全局工具。补齐 Docker、Kind 和 kubectl 后，应按以下顺序重跑，只有全部 mandatory assertions 成功才能把本节改为 PASS：

```bash
make kind-serving-up
make test-kind-serving
make kind-serving-down
```

### 其他外部边界

- 没有真实 NVIDIA GPU 或 vLLM 验证；Phase IV-A 只支持 development/test Fake inference Pod。
- 没有多物理节点、HA PostgreSQL/Redis、公网部署或生产 Kubernetes 安全审计。
- 远端 CI 尚未运行；本报告只记录本机 WSL2 结果。

## 验证中发现并修复的问题

1. 初版 409/restart adoption 只校验部分字段，可能接管标签正确但 workload 或 Service 端口错误的资源。现已改为可从实际 Pod 重算的完整 contract hash，并增加漂移回归测试。
2. 初版 PostgreSQL manifest 把 `emptyDir` 根目录直接作为 non-root `PGDATA`，UID 70 无权调整根目录权限。现改用可自行创建并拥有的 `pgdata` 子目录，真实镜像 `initdb` smoke 已通过。
3. 新增 repository 测试曾有一个 mypy 可空赋值错误；拆分变量后，完整 mypy 和两轮 pytest 均重新通过。

## 结论

Kubernetes-native serving backend、生命周期控制、恢复、Gateway 接入和可执行 Kind E2E 资产已经实现，并通过本机静态、单元、SQLite integration、Compose/YAML 与容器权限 smoke。由于缺少 `kind` 和 `kubectl`，本机没有产生真实 Kubernetes HTTP/SSE、扩缩容和恢复的运行证据；该限制必须保留为 `NOT RUN`。
