# Roadmap

本路线图表达当前优先级，不是交付日期或生产支持承诺。每项能力只有在代码、自动化检查和对应环境证据齐备后，才会更新对外声明。

## Current: project contract

- 统一包、CLI、Compose、镜像与版本身份；为旧入口提供有期限的兼容期。
- 建立许可证、安全报告、贡献、发布和证据声明规范。
- 保持现有调度、服务、fencing 与 API 行为不变，并用独立 PR 收口。

## Next: reproducible evidence

- 固化核心演示的输入、环境、commit SHA、产物与结果摘要。
- 缩小 mock、单机 Docker、Kind 与真实外部后端之间的证据缺口。
- 为性能、恢复时间和隔离边界增加可比较的基线，而不是使用无来源的“生产级”描述。

## Later: controlled hardening

- 按已验证风险推进节点身份、mTLS、细粒度网络策略和外部对象存储数据面。
- 在明确运行环境后验证升级、回滚、备份恢复和控制器高可用边界。
- 只有在实测支持时才扩大规模、可靠性与兼容性声明。

具体候选工作应通过 Issue 和独立 PR 进入；路线图不覆盖 [verification matrix](docs/verification-matrix.md) 中的实时状态。
