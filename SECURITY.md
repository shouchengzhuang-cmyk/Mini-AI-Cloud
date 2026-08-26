# Security policy

## Supported versions

安全修复以默认分支和最新已发布版本为优先目标。历史标签仅作为可复现恢复点，不承诺持续回补；使用者应先在最新版本复现问题。

本仓库是实验性控制面，不应直接承载生产凭据或暴露在公网。发现问题不代表已有生产支持或响应 SLA。

## Report a vulnerability

请使用仓库 Security 页面中的 [private vulnerability reporting](https://github.com/shouchengzhuang-cmyk/Mini-AI-Cloud/security/advisories/new) 私下报告。不要为未修复漏洞创建公开 Issue，也不要在报告中提交真实 API key、密码、私钥、Cookie、数据库快照或其他敏感数据。

报告应尽量包含：

- 受影响的 commit SHA、版本与运行环境；
- 最小复现步骤和预期/实际结果；
- 影响范围、所需权限与已知缓解方式；
- 已脱敏的日志、请求或测试用例。

维护者会尽力确认问题、评估影响并协调修复与披露时间，但不承诺固定响应时限。若问题只是一般缺陷且不涉及保密性、完整性或可用性风险，请改用普通 Issue。
