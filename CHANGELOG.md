# Changelog

本项目的重要变更记录在此文件中。格式参考 Keep a Changelog，版本遵循 [release policy](docs/release-policy.md) 中定义的语义化版本规则。

## [Unreleased]

暂无。

## [0.6.0] - 2026-08-30

### Added

- 单集群 Helm Chart，包含单副本 control plane、Worker、migration hook、外部 PostgreSQL/Redis/Secret 接线，以及 system/workload namespace 的有界 RBAC。
- 以 `batch/v1` Job 执行 Kubernetes batch task，并保留 execution fencing、restart adoption、精确 UID 删除和漂移隔离合同。
- 将 release-pinned Runtime Profile manifest 正式挂载到 workload renderer，提供 NVIDIA 与 Huawei Ascend 的静态 placement contract、vendor preflight 和 OpenAI-compatible serving 路径。
- v0.6 Kubernetes adaptation 权威文档、发布准备状态与支持证据矩阵，以及要求 exact SHA、Chart digest、claim、cleanup 和 checksum 的 P4 evidence gate。

### Changed

- production Kubernetes task 在 API 调用前拒绝 artifact-bearing Job；不再把 development/test 的 pinned `hostPath` 路径外推成生产跨节点 artifact 数据面。
- release gate 的版本基线升级为 `0.6.0`，前序 tag 固定为 `v0.5.0`；Helm Chart、应用身份、锁文件、发布工作流与兼容快照保持一致。
- Owner 授权发布流程必须先取得真实、clean、commit-bound 的 `KIND_K8S_PASS` bundle，再准备或发布 release assets。

### Evidence boundary

- 当前 P5 状态为 `READY_FOR_OWNER_AUTHORIZATION`，但最终 P4 Kind bundle 引用仍是 `PENDING_FINAL_P4_EVIDENCE`；缺少或漂移的 bundle 会阻止 `prepare`。
- 真实 NVIDIA 与真实 Huawei Ascend 硬件均为 `REAL_HW_NOT_RUN`，不声明真实推理、吞吐、延迟或故障切换表现。
- 本条目是 `NOT_TAGGED`、`NOT_RELEASED`、`NOT_DEPLOYED` 的发布准备，不代表 tag、GitHub Release 或生产部署已经发生。

## [0.5.0] - 2026-08-29

### Added

- 厂商中立 accelerator 请求、持久化与 allocation-authority 合同，以及可插拔 NVIDIA、Huawei Ascend、Kubernetes inventory providers。
- 不可变 Runtime Profile、logical model / physical variant、NVIDIA vLLM 与 Huawei Ascend A2 profile 和验收记录。
- accelerator-aware Kubernetes serving renderer、vendor-aware admission/quota/fencing，以及 strict、preferred、balanced 双厂商路由、fallback、circuit 和物理用量归因。
- NVIDIA + Huawei Ascend 双后端 benchmark harness，覆盖 buffered/SSE、语义校验、TTFT 和独立 fallback drill 阶段。
- A1-A11、post-A11 correctness hardening、benchmark review fixes 与开放堆叠 PR #20-#27 的机器可检覆盖表。

### Changed

- Kubernetes inventory 绑定 Runtime Profile hardware family、taints/tolerations，并按 scheduler 语义扣除外部 Pod requests。
- 服务准入、扩缩容、reconcile、worker heartbeat、gateway 并发与 migration upgrade 改为 fail-closed，补充稳定锁顺序、placement snapshot 和隔离 PostgreSQL 回归。
- benchmark 的 SSE TTFT 只从首个非空 content delta 起算；exact prompt 比较完整归一化响应；fallback drill 与 baseline 测量分阶段执行。

### Evidence boundary

- Runtime Profile、静态验证、模拟 fixture、PostgreSQL/Redis integration 与单节点 Kind 路径已具备仓库内证据；其结论不得外推为生产 HA 或多物理节点能力。
- 真实 NVIDIA 与真实 Huawei Ascend 硬件均为 `REAL_HW_NOT_RUN`，不声明真实吞吐、延迟、语义等价或故障切换表现。
- 不声明 universal hardware compatibility、SLA、production-ready cloud 或完整 Kubernetes-native platform 支持。
- 该条目只准备 v0.5.0；合并 `main`、tag、GitHub Release 与任何部署仍需各自的明确授权和精确 SHA 门禁。

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
