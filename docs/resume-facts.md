# Resume facts: Mini AI Cloud

本文件只记录能由仓库、测试或 commit-bound 产物复核的事实。它不把本地/Kind 验收外推为生产部署或真实 GPU 结论。

## Architecture

- Python 3.12、FastAPI、PostgreSQL、Redis、Docker Compose、Kubernetes/Kind、Prometheus 和 Grafana 组成单机开发与隔离验收栈。
- API、worker、scheduler、runtime、serving controller 和 CLI 通过明确的 repository/service 边界协作；Docker 与 Kubernetes runtime 使用同一 execution lifecycle 语义。
- 当前 release 准备版本是 `0.4.0`；仓库仍保留兼容的 `mini-docker-cloud` CLI 别名。

## Correctness and safety

- Task lease、worker session、execution fencing、reservation/adoption 和 reconciliation 都有单元或集成测试覆盖。
- PostgreSQL 并发测试验证数据库事务路径；Docker 和 Kind 验收与纯 mock/unit 测试分开标记。
- bounded soak 会重复执行 restart/adoption/fencing，并检查旧 session/execution 不能提交终态。
- DR rehearsal 只创建并删除带唯一 project label 的临时 PostgreSQL/Redis/MinIO volumes，校验 marker 恢复后再清理；它不触碰默认开发栈 volume。

## Verified surface

- 当前源码树由 pytest 收集 `609` 个测试；最终 PASS 状态以同一 release SHA 的 `make test-release` 输出和 `build/evidence/<git-sha>/manifest.json` 为准。
- `make test-release` 串联 lock、Ruff、mypy、evidence schema、Compose config、完整 pytest、wheel 独立安装、非 root container smoke、真实隔离 Kind serving E2E、commit-bound evidence 和 release-preparation bundle。
- OpenAPI 与 CLI v1 完整快照、锁定依赖清单、GitHub Action SHA pin、secret pattern scan、CycloneDX SBOM 和容器基线均由 release gate 检查。
- hero runner 覆盖 stale worker/execution fencing、controller restart adoption 和 active SSE drain；每条 claim 必须与 evidence contract 和 verification matrix 对齐。

## Evidence boundary

- 本地 PostgreSQL、Docker daemon 和单节点 Kind 是真实执行环境，但不是生产 HA 或多物理节点 Kubernetes 证据。
- 没有运行真实 NVIDIA GPU/vLLM acceptance 时，所有 release manifest 和 release notes 必须保留 `REAL_GPU: NOT_RUN`。
- release preparation 明确记录 `NOT_CREATED` 和 `NOT_DEPLOYED`；它不创建 GitHub Release，也不部署服务。
- 不声称替代 KServe、Volcano、Ray Serve 或托管云平台；[`comparison.md`](comparison.md) 只比较职责边界。
