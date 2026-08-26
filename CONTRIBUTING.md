# Contributing

感谢参与 Mini AI Cloud。仓库优先接受范围清晰、证据可复现且不夸大能力边界的改动。

## Before you start

1. 先搜索现有 Issue 和 PR；较大改动应先开 Issue 说明问题、非目标和验证计划。
2. 从最新默认分支创建短生命周期分支，不要在功能 PR 中夹带无关重构、依赖升级或文件搬迁。
3. 不要提交真实凭据、生产数据、个人信息或无法公开的日志。

## Local setup

项目要求 Python 3.12、`uv`；涉及容器链路时还需要 Docker Engine 与 Docker Compose v2。

```bash
uv sync --frozen --all-groups
make check
docker compose config --quiet
```

根据改动范围补充集成、Docker 或 Kind 验证。缺少外部环境时必须写 `NOT RUN` 和原因，不能把 mock、代码审查或命令存在写成真实运行通过。

## Change requirements

- 保持数据库状态真相、lease、fencing、租户隔离与最小权限边界；行为改动需增加相应测试。
- 使用 Conventional Commits，例如 `fix(worker): reject stale execution writes`。
- 更新面向用户的文档和 [CHANGELOG.md](CHANGELOG.md)；纯内部、无用户影响的维护项可只记入 Unreleased 的 Changed。
- 按 [claim policy](docs/claim-policy.md) 标记 `REAL`、`SIMULATED` 或 `NOT RUN` 证据。
- 遵循 [release and deprecation policy](docs/release-policy.md)，不要未经迁移期删除公开入口或配置。

## Pull requests

PR 必须说明变更内容、解决的问题、非目标、验证方式、风险与证据。只列出实际执行过的命令和结果；若需要部署、数据库迁移或凭据变更，应明确标注并在合并前复核。维护者可能要求拆分无法独立审查的混合改动。
