# Version, release and deprecation policy

## Versioning

Mini AI Cloud 使用语义化版本表达公开包、CLI、配置和 API 契约：

- `MAJOR`：需要明确迁移的破坏性变化；
- `MINOR`：向后兼容的功能或行为扩展；
- `PATCH`：向后兼容的缺陷、安全或文档修复。

开发快照使用 PEP 440 预发布形式，例如 `0.4.0.dev0`。`0.x` 阶段仍可能快速演进，但已公开入口的破坏性变化也必须记录、提供迁移说明并执行弃用流程。

## Release gate

发布前至少满足：

1. 候选 commit 位于干净、可追踪的默认分支历史中；
2. 包元数据、运行时版本、CLI 帮助和文档一致；
3. 适用的格式、lint、类型、单元、集成与环境测试通过，未执行项明确标为 `NOT RUN`；
4. CHANGELOG、迁移说明、风险与证据链接已更新；
5. 不包含已知泄露凭据或未处置的高风险安全问题；
6. tag 和构建产物只从已确认的 release commit 生成。

发布、合并、推送和部署是不同动作。通过 CI 不自动授权发布或部署。

## Owner-approved GitHub publication

仓库提供受限的 `Publish approved release` 工作流，用于在代码和证据已经收口后创建 Git tag 与 GitHub Release：

1. 必须由仓库 Owner 在普通 Issue 中提交精确命令：`/publish-release <version> <40-char-main-sha>`；
2. 工作流只接受当前默认分支 head，拒绝功能分支、过期 commit、非 Owner 评论和非精确版本；
3. 精确 SHA 必须已有成功的 `CI` 与 `Release security gates`；
4. 发布前重新执行完整 release gate、真实 Kind acceptance、bounded soak、隔离 DR rehearsal、secret/vulnerability scan，并生成 wheel、SBOM 和 commit-bound evidence；
5. 工作流可幂等修复同一 tag 的 Release 资产，但拒绝让 tag 指向其他 commit；
6. GitHub Release 只发布仓库制品，不执行生产部署，也不改变 `REAL GPU: NOT RUN` 等证据边界。

Owner 的精确命令构成 tag/GitHub Release 的显式授权，但**不构成部署授权**。任何部署仍需独立、明确的目标环境与风险确认。

## Deprecation

公开 CLI、环境变量、配置键或 API 被替代时：

1. 先提供新入口和迁移说明；
2. 旧入口继续工作并输出可识别的弃用提示；
3. 至少保留一个已公告的开发或发布周期；
4. 在 CHANGELOG 和计划移除版本中记录；
5. 只有兼容测试和迁移文档齐备后才允许删除。

安全问题可能需要缩短兼容期；此时必须说明风险、替代方案和影响范围。
