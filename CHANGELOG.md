# Changelog

本项目的重要变更记录在此文件中。格式参考 Keep a Changelog，版本遵循 [release policy](docs/release-policy.md) 中定义的语义化版本规则。

## [Unreleased]

暂无。

## [0.4.0] - 2026-08-26

### Added

- 项目许可证、安全报告流程、贡献指南、路线图与 GitHub 协作模板。
- `REAL`、`SIMULATED`、`NOT RUN` 证据声明规范。
- commit-bound evidence bundle、受限 soak、隔离破坏性 DR rehearsal 和三个 Hero Scenario。
- release gate、OpenAPI/CLI 兼容快照、CycloneDX SBOM 与自动 release-notes 准备包。
- 仅接受仓库 Owner 精确 Issue 命令、当前默认分支 SHA 和完整重新验收的 GitHub Release 发布工作流。

### Changed

- 明确版本、发布、兼容与弃用策略。
- Python package、CLI、Compose 和镜像身份统一为 Mini AI Cloud 0.4.0。
- Kubernetes serving 改用预置 headless Service 的 Pod 专属 DNS，controller 不再拥有 Service 写权限。
- 运行镜像移除仅供构建使用的 pip vendor 工具链，缩小依赖与漏洞面。

### Security

- 将测试依赖锁定到已修复 CVE-2025-71176 / GHSA-6w46-j5rx-g56g 的 pytest 9.1.1，并保持 `pytest>=9.0.3,<10` 的安全版本下限。
- GitHub Release 发布前重新执行 secret、依赖、文件系统与容器扫描；发布动作只接受 Owner、精确版本和当前 `main` SHA。

该版本条目表示仓库已完成 0.4.0 元数据与受控发布准备；生产部署仍需独立授权。版本发布时只记录仓库历史可证实的变更。
