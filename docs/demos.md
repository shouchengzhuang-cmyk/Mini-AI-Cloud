# Phase II 强制演示手册

每个 Demo 都把“代码/隔离测试”与“真实外部集成”分开。只有命令实际执行并保留输出后，才能在最终报告写 `actually tested`。

## 共用准备

```bash
cp .env.example .env
docker compose config --quiet
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/readyz
```

全新数据库按 README bootstrap，保存一次性 API Key 到安全环境变量，并为演示镜像配置最小 allow rule：

```bash
export MINI_CLOUD_API_KEY='<bootstrap key>'
```

不要把 API Key、Secret、数据库密码或 presigned URL 粘进演示输出。每次记录：Git commit、UTC 时间、命令、exit code、关键 UUID 和明确断言。

## Demo 1：Legacy Task

目标：旧客户端不带 API Key 提交普通 Docker CPU task，输出 hello，终态 succeeded。

```bash
uv run python scripts/e2e_demo.py \
  --base-url http://localhost:8000 \
  --timeout 180
```

验收：

- 找到 online Docker Worker；
- POST `/api/v1/tasks` 返回 201；
- SSE 收到 stdout 和 end；
- Task `status=succeeded`、`exit_code=0`、`execution_id` 非空；
- PostgreSQL 持久日志顺序正确；
- 脚本打印 `assertions: passed` 且 exit 0。

若设置了 `LEGACY_ANONYMOUS_ENABLED=false`，这个兼容 Demo 应在专用 local stack 临时开启，不能因此放宽共享环境。

## Demo 2：Authenticated Project Task 与 Artifact

目标：认证 Project task、quota、logs、timeline、usage/cost、artifact checksum 全链路。可选第二 Project key 验证隔离。

```bash
export MINI_CLOUD_API_KEY='<owner/admin key>'
# 可选，必须属于另一个 Project：
export MINI_CLOUD_OTHER_PROJECT_API_KEY='<other project key>'

uv run python scripts/phase2_demo.py \
  --base-url http://localhost:8000 \
  --image python:3.12-slim \
  --timeout 180
```

脚本执行：

1. `/readyz` 和 `/auth/whoami`；
2. 读取 Project quota 并评估 image policy；
3. 提交唯一 idempotency marker task，等待 succeeded；
4. 在持久日志找到 marker，timeline 非空；
5. usage/cost 返回同 Project 且 ledger 至少包含本 execution；
6. metadata → upload grant → streaming upload → finalize → download → SHA-256；
7. 默认删除脚本创建的 artifact metadata/object；`--keep-artifact` 可保留；
8. 提供第二 Project key 时，断言读取 task 返回 404。

成功输出不含 credential，最后一行 `assertions: passed`。

补充自动化证据：

```bash
uv run pytest \
  tests/integration/test_identity_api.py \
  tests/integration/test_task_artifact_workspace.py \
  tests/unit/test_artifact_routes.py \
  tests/unit/test_artifact_service.py \
  tests/unit/test_executor_artifacts.py \
  tests/unit/test_docker_runtime.py \
  tests/unit/test_kubernetes_runtime.py \
  tests/unit/test_phase2_demo.py -q
```

`phase2_demo.py` 验证独立 Artifact API；Task-bound input/output 的真实执行证据来自 `tests/e2e/test_docker_runtime.py` 中的 Docker named-volume `Subpath` input→container→output 测试。裸机单文件 bind 与 Kubernetes pinned `hostPath(type=File)` 仍是 spec/单元证据；Kubernetes 必须单独标注“未做 Kind 文件可见性验证”。

## Demo 3：GPU Scheduling

目标 inventory：Worker A = 4×A100，Worker B = 1×RTX4090。验证 2×A100 任务只能选 A，RTX4090 任务只能选 B，具体 device 不被双重分配。

无 GPU 机器先运行 deterministic Fake inventory 和 policy tests：

```bash
uv run pytest \
  tests/unit/test_gpu_inventory.py \
  tests/unit/test_scheduler_policies.py \
  tests/integration/test_global_preemption.py -q
```

再运行实际 100 nodes / 4 GPUs / 10,000 jobs 模拟，不预设 binpack 一定更好：

```bash
make benchmark
```

验收：

- production 配置拒绝 `FAKE_GPU_COUNT>0`；
- GPU model/memory/count rejection reason 稳定；
- assignment 保存具体 UUID，而不是 `--gpus all`；
- active ReservationGPUDevice 唯一；
- 模拟输出 JSON/CSV，含 utilization、latency、makespan、fragmentation/preemption 指标；
- 最终报告引用本次真实输出，而不是硬编码结论。

真实 NVIDIA E2E 只有在节点具备 driver、NVIDIA Container Toolkit 和可用设备时才执行；否则明确写 fake/simulation evidence。

## Demo 4：Fencing

目标：A 的旧 execution 在 lease 丢失、B 获得新 execution 后不能覆盖 B。

快速 repository proof：

```bash
uv run pytest \
  tests/integration/test_task_repository.py::test_stale_execution_result_cannot_overwrite_new_owner \
  -q
```

真实 Compose fault：

```powershell
./scripts/fault_injection.ps1 -Case StaleWorkerResult -Confirm:$false
```

验收：old execution completion 返回 `accepted=false`，Task 当前 `execution_id` 仍是 B，reservation/usage 不被旧结果重复释放或结算。

## Demo 5：Preemption

目标：低优先级、显式 `preemptible=true` 的 GPU task 被高优先级任务抢占，且 GPU 不在旧容器停止前提前重分配。

```bash
uv run pytest \
  tests/integration/test_global_preemption.py::test_preemption_releases_gpu_only_after_fenced_worker_result \
  -q
```

验收顺序：

```text
low task owns GPU reservation
high task creates durable preemption intent
low task -> preempting
old execution fenced stop result arrives
GPU reservation releases exactly once
high task can be placed
low task follows configured requeue/terminal policy
```

单测/SQLite integration 通过不等于真实 Docker GPU 抢占；有 GPU 环境还要记录实际容器停止、device visibility 与 `nvidia-smi` 证据。

## Demo 6：Service Reconciliation

目标：创建 `replicas=2` 的 Fake inference service，杀死一个 replica 后 controller 创建 replacement，最终 desired=2、healthy=2。

自动化控制面证据：

```bash
uv run pytest \
  tests/unit/test_fake_replica_runtime.py::test_fake_runtime_monitors_unexpected_process_exit \
  tests/unit/test_service_repository.py::test_expired_replica_lease_is_fenced_and_replaced \
  tests/unit/test_gateway.py::test_health_threshold_marks_unhealthy_then_reconciles_replacements \
  tests/unit/test_gateway.py::test_gateway_routes_round_robin_filter_headers_and_stream \
  -q
```

真实本地 API 演示应使用 `APP_ENV=development`、`runtime_type=fake`：

1. POST `/api/v1/services`，`replicas=2`。
2. 轮询 `/api/v1/services/{id}/replicas` 至两个 healthy。
3. 精确终止一个 fake replica process（不要杀其他任务或服务）。
4. 记录旧 replica execution/generation。
5. 轮询 replacement；确认旧 replica 不再进入 gateway candidate。
6. 连续请求 gateway，确认只路由 healthy replicas，最终 desired/actual/healthy 均为 2。

无 NVIDIA GPU 时这证明控制面，不证明真实 vLLM 吞吐、显存占用或模型正确性。

## Demo 7：灾备恢复

目标：专用测试栈创建历史数据，backup，模拟 DB volume 丢失，restore 后仍能查询。

### 安全前置条件

- 使用新 run id：`mini-ai-cloud-local-dr-<run-id>`；
- Project name、Compose labels、volume name 三者都要匹配；
- backup 目录在该 run 外且 checksum 验证通过；
- 禁止对默认日常栈、共享 DB、未知 volume、workspace root 使用删除命令；
- volume 删除是破坏动作，必须由操作者在现场再次确认精确目标。

流程：

```bash
DR_PROJECT='mini-ai-cloud-local-dr-<run-id>'
BACKUP_ROOT="$PWD/build/dr-backups"

docker compose --project-name "$DR_PROJECT" up --build -d
# 运行 Demo 1/2，并保存 task/artifact marker 与 SHA-256
docker compose --project-name "$DR_PROJECT" stop api worker minio
bash scripts/backup.sh \
  --local-stack \
  --project-name "$DR_PROJECT" \
  --output-dir "$BACKUP_ROOT"
```

下一步不要复制粘贴通配删除。先用 `docker volume ls` 和 `docker volume inspect` 确认唯一 PostgreSQL volume 的两个 Compose labels，再由操作者删除该精确名字。然后：

```bash
docker compose --project-name "$DR_PROJECT" stop
bash scripts/restore.sh \
  --local-stack \
  --confirm-overwrite \
  --project-name "$DR_PROJECT" \
  --backup-dir '<上一步生成的精确绝对目录>'
docker compose --project-name "$DR_PROJECT" up -d
```

验收：

- `/readyz=200`，migration revision 正确；
- 原 task id、terminal status、timeline、logs、execution history 可查；
- usage ledger 未重复、terminal task 无 active reservation；
- artifact bytes 与原 SHA-256 一致；
- 使用恢复后的外部 key ring 才能验证 Secret；数据库 backup 本身不包含 master key；
- 最后清理专用 DR stack，并保留脱敏报告和 `SHA256SUMS`。

未执行 volume 删除/restore 时，只能报告“backup/restore scripts validated or dry-run inspected”，不能把脚本存在写成灾备演练通过。

## 完整收口

七个 Demo 之后再运行：

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy .
uv run pytest -m "not slow"
uv run pytest
docker compose config --quiet
```

最终报告结构和每项环境证据见 [验证矩阵](verification-matrix.md)。
