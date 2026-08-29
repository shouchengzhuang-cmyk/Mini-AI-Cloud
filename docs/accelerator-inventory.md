# Accelerator inventory providers

A3 将设备发现收敛为 `InventoryProviderRegistry`。每个 provider 输出同一个
`AcceleratorDevice` 值对象，v0.4 内部的 `GPUDevice` 名称只保留为兼容 alias。

## Provider 与配置

`ACCELERATOR_INVENTORY_PROVIDERS` 是逗号分隔的已知 provider 列表：

| 名称 | 数据源 | 设备合同 |
| --- | --- | --- |
| `nvidia-smi` | 有界 `nvidia-smi --query-gpu ... --format=csv,noheader,nounits` | `nvidia/gpu` |
| `ascend-npu-smi` | `npu-smi info -m` 与按 NPU ID 查询 memory | `huawei-ascend/npu` |
| `kubernetes-node` | Node allocatable/labels 与已绑定 Pod 的 accelerator requests | 不可直接调度的空闲 capacity slot |
| `fake` | 确定性本地生成 | 仅 development/test |
| `none` | 显式禁用发现 | 健康的空 inventory |

默认值仍为 `nvidia-smi`，保持 v0.4 部署行为。`fake` 和 `none` 必须单独使用；
`FAKE_GPU_COUNT>0` 保留 v0.4 的 fake override。production 配置同时拒绝非零
`FAKE_GPU_COUNT` 和显式 `fake` provider。

## 可观测状态

- `available`：命令/数据源可用，输出可以是 0 个设备。
- `degraded`：数据源可读，但有坏行、缺少 memory metadata 或部分子查询失败；
  保留已验证的设备并报告拒绝数。
- `unavailable`：命令不存在、超时、非零退出、UTF-8 解码失败或输出超限。

`unavailable` 不等于“健康的 0 卡节点”。Worker 启动时会为每个 provider 记录
status、device count、rejected rows 和非敏感 reason code。调用不会把 stderr 或完整
环境变量写入日志。单次命令默认超时 5 秒，输出上限 1 MiB，解析上限
256 行/容量槽。

## 厂商解析边界

NVIDIA provider 以 GPU UUID 作为稳定身份，不以可能在重启后变化的枚举顺序作为
身份。实现依据 [NVIDIA nvidia-smi 文档](https://docs.nvidia.com/deploy/nvidia-smi/index.html)
使用 CSV query 和 UUID。

Ascend provider 先解析 mapping 表的列名，再依 NPU ID 读取 memory key/value，不绑定某一
版本的列位置。命令边界对应华为官方的
[`npu-smi info -m`](https://www.hiascend.com/document/detail/zh/Atlas%20200I%20A2/2520/re/npu/npusmi_013.html)
与 [memory 查询](https://www.hiascend.com/document/detail/zh/Atlas%20200I%20A2/253RC1/re/npu/npusmi_017.html)。
mapping/memory 查询不能证明芯片 health，因此 A3 保守持久化为 `unknown`，不伪造
`healthy`。

## Kubernetes capacity 不是运行时设备观测

Kubernetes provider 识别 `nvidia.com/gpu`、`huawei.com/Ascend*` 和 `huawei.com/npu`，
并排除 memory/core/fault/recovery 等辅助 resource。Ascend 的节点标签和资源语义参考
[MindCluster Kubernetes API](https://github.com/Ascend/mind-cluster/blob/master/docs/en/scheduling/api/k8s.md)。

Node allocatable 不提供物理 device ID，因此 provider 生成
`k8s-capacity:<node-uid>:<resource>:<slot>` 的稳定容量槽 ID，并将 health 标记为
`inventory-only`。这些 ID 不参与现有 exact-device 调度，也不得写成 execution 的
observed device IDs；后者仍只能由 Pod/Device Plugin 运行态观测写回。

Node allocatable 是静态可分配上限，不会随 Pod 绑定自动递减。provider 会同时列出目标
节点上的非终态 Pod，并扣除外部 Pod 的 accelerator requests（未显式 request 时使用
对应 limit）。具有匹配 cluster/worker 所有权标签的 Mini AI Cloud Pod 由数据库中的
service/reservation commitment 统一计费，避免重复扣减。Pod 列表不可读、资源数量畸形
或请求超过 allocatable 时，provider 会 fail closed，不发布可调度容量。外部占用的容量槽
保留节点/profile 兼容性元数据，但 health 标记为 `externally-allocated`，不会作为空闲容量
参与正副本或 batch 准入。

因此运行 `kubernetes-node` provider 的 Kubernetes 身份必须同时拥有目标 Node 的
`get` 权限和所有 namespace Pod 的 `list` 权限；使用 kubeconfig 时也必须授予等价权限。

## 证据边界

- NVIDIA CSV fixture：`SIMULATED`。
- Ascend 列顺序变体与 memory fixture：`SIMULATED`。
- Kubernetes Node JSON fixture：`SIMULATED`。
- 本机没有执行真实 NVIDIA/Ascend 命令：`REAL_HW_NOT_RUN`。
