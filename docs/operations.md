# 部署、回滚、备份恢复与排障

本手册针对一次性/本地 Compose 与受控测试环境。它不把单机 Compose 描述成生产 HA 部署。

当前默认 Compose project 为 `mini-ai-cloud`。Docker Compose 不会自动重命名旧的 `mini-docker-cloud` project 或 volume；如需迁移已有本地栈，先完成备份，再用 `docker compose --project-name mini-docker-cloud down` 显式停止旧栈，并人工确认新栈应连接的数据。不要在未核对 volume 的情况下删除旧 project。

## 部署模式

| 模式 | 用途 | 关键边界 |
| --- | --- | --- |
| 默认 Compose | Local Artifact、一个 Docker Worker、API/控制循环 | `artifact-data` 保存对象，`artifact-workspace-data` 通过单文件 Subpath 交付 task；真实 Docker E2E 已通过 |
| `artifacts` profile | MinIO/S3-compatible Artifact | 需要单独验证 presigned upload/download |
| `observability` profile | Prometheus + Grafana | 仅 localhost；不是告警平台 |
| Host API/Worker | 调试 Python 进程 | 与 daemon 共享宿主路径时使用单文件 bind；URL host 应为 `localhost` |
| Kind | Kubernetes runtime 测试 | 不永久切换用户既有 context；测试后清理 |

默认 Compose 同时把 `artifact-data` 挂给 API 与 Worker，让 Worker 能读取 Local Artifact bytes；Worker 的 execution workspace 则位于 `artifact-workspace-data`。若未显式设置 `DOCKER_ARTIFACT_WORKSPACE_VOLUME`，实际卷名从 `COMPOSE_PROJECT_NAME` 派生为 `<project>-task-workspaces`，因此专用测试/DR stack 不会与默认栈共享 workspace。Worker 通过 Docker socket 创建 sibling task container 时，只用 `VolumeOptions.Subpath` 暴露声明的单个 input/output 文件，避免把 Worker 容器内绝对路径当成 daemon 宿主 bind 路径。裸机 Worker 才走宿主单文件 bind；Kubernetes 路径仍是 pinned node 的 `hostPath(type=File)`。配置、mount options 与真实 Docker Subpath input→container→output E2E 均已有证据。

启动前检查：

```bash
cp .env.example .env
docker compose --env-file .env config --quiet
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8000/livez
curl -fsS http://localhost:8000/readyz
```

`/livez=200` 只证明进程活着；接流量前必须确认 `/readyz=200`、migrate 成功、至少一个需要的 Worker online。

## 必须显式设置的共享环境配置

`.env.example` 中的密码和 pepper 只供 localhost 开发。任何共享环境至少应显式提供：

- `APP_ENV=production`
- `LEGACY_ANONYMOUS_ENABLED=false`
- `BOOTSTRAP_ENABLED=false`（完成首次初始化后）
- 强随机 `API_KEY_PEPPER`
- 强随机 `WORKER_AUTH_TOKEN`（为未来 internal worker API 预留）；当前 Worker 是 DB/Redis 直连，必须另外使用独立数据层凭据与网络 ACL，不能把该 token 当成已经生效的节点认证
- 唯一 `CLUSTER_ID`
- 非默认 PostgreSQL、Redis、MinIO credential
- 与部署密钥管理系统集成的 `SECRET_MASTER_KEY`
- 明确的 image policy、request/rate limit、artifact limit 与 retention 值

检查 `docker compose config` 的渲染结果，确认每个变量确实透传到目标 service；只写进 `.env` 但 compose 未引用不算配置生效。不要把渲染后的 secret 输出到 CI 日志。

### Secret key ring

格式是逗号分隔的 `key-id:base64-32-byte-key`，第一项是当前写入 key：

```text
SECRET_MASTER_KEY=new-v2:<base64-32-byte-key>,old-v1:<base64-32-byte-key>
```

轮换顺序：

1. 生成新 32-byte key，放在 key ring 第一项；旧 key 保留。
2. 部署并确认新写入使用新 key id、旧 Secret 仍可解密。
3. 通过受审查的 re-encryption 流程迁移历史版本。
4. 查询确认数据库不再引用旧 key id 后才能移除旧 key。

丢失仍被引用的 key 会永久失去对应 Secret 明文。备份数据库不等于备份 KMS/key ring，应分别保护并演练恢复。

## Migrations

Compose 的 `migrate` service 自动执行：

```bash
alembic upgrade head
```

宿主执行：

```bash
export DATABASE_URL='postgresql+asyncpg://...@localhost:5432/task_platform'
uv run alembic current
uv run alembic upgrade head
uv run alembic check
```

升级检查清单：

1. 对 Phase I schema 和带 active task 的副本做备份。
2. 在专用测试数据库完成 `0002_worker_reservations -> head`。
3. 检查 active execution、resource reservation 和 quota backfill。
4. `alembic check` 不应产生未迁移的模型差异。
5. 先迁移，再启动新 API/Worker；失败时保留 migration 日志和 DB snapshot。

## 代码回滚与数据库回退

Phase I 稳定代码恢复点为 `c47702b`，但“代码能切回”不等于“Phase II 数据可无损降级”。推荐策略：

1. 停止接受新写入并停止 Worker/controller。
2. 创建并验证 backup checksums。
3. 若 migration 已成功、只是应用错误，优先部署 forward fix；不要随意 downgrade 有业务数据的数据库。
4. 只有在一次性测试栈才能演练 `alembic downgrade 0002_worker_reservations`；它会删除 Phase II 表/列，属于数据破坏操作。
5. 如需回到 Phase I，应恢复升级前 snapshot，再运行 `c47702b` 代码，并明确丢弃升级后的写入窗口。

不要把 `git reset --hard` 当作回滚流程；它会破坏工作树且不能恢复数据库/Object Store。

### Accelerator allocation migration rollback

`0011_accelerator_persistence` 是增量迁移：保留 `gpu_devices`、legacy `gpu_*`
字段和 reservation 关联表，只追加 vendor/kind、runtime profile、allocation authority
及 observed allocation 列。升级前后应运行：

```bash
uv run alembic current
uv run alembic upgrade 0011_accelerator_persistence
uv run mini-cloud admin doctor
```

升级后的只读核查至少包括：

- v0.4 GPU 行映射为 `nvidia/gpu`，fake GPU 的 provenance 仍由 `fake=true` 保存；
- 已有 concrete device link 能回填 observed device IDs；缺少绑定的历史 GPU
  reservation 标记为 `legacy_unbound`，不能伪造观测；
- `orphan_accelerator_allocation` 没有新增问题；
- terminal task 不存在 active reservation 或 active exact-device link。

若应用错误但 schema 已成功，优先 forward fix。仅在已停止所有写入、完成可恢复备份并确认
没有需要保留的 A2 新写入后，才可演练：

```bash
uv run alembic downgrade 0010_ai_serving_infrastructure
```

该 downgrade 会删除 runtime profile、allocation authority 和 observed allocation 证据，无法还原
升级后的 Kubernetes Device Plugin 观测；生产恢复应使用升级前 snapshot，而不是把 downgrade
当作无损回滚。如果同一 Worker 已有不同 vendor 共用 `device_index` 的合法数据，
downgrade 恢复 v0.4 唯一键时会被数据库拒绝；应停止回退并使用升级前 snapshot。

## Local backup

`scripts/backup.sh` 只接受受限的本地 Compose project 名，并备份 PostgreSQL custom dump、Local Artifact volume 和可选 MinIO volume。为了得到一致 snapshot，postgres 保持运行，API、Worker、migrate 和 MinIO 必须停止：

```bash
docker compose --project-name mini-ai-cloud stop api worker minio
make backup
```

输出目录含：

```text
manifest.json
postgres.dump
artifact-data.tar.gz  # 若 volume 存在
minio-data.tar.gz     # 若 volume 存在
SHA256SUMS
```

脚本拒绝空 dump、多重匹配 volume 和不受支持的 project 名。备份成功后才能恢复服务：

```bash
docker compose --project-name mini-ai-cloud up -d api worker
```

## Local restore

Restore 会覆盖目标测试栈的数据库与 artifact volume，必须显式确认：

```bash
docker compose --project-name mini-ai-cloud stop
make restore BACKUP=/absolute/path/to/backup CONFIRM_RESTORE=YES
docker compose --project-name mini-ai-cloud up -d
curl -fsS http://localhost:8000/readyz
```

恢复脚本在写入前验证 `SHA256SUMS`、manifest、archive 路径与 link，拒绝未停止的目标 stack 和非本地 project 名。Restore 完成后至少核查：

- `alembic current` 与应用版本匹配；
- 历史 Task/timeline/log 可查询；
- Project、Membership、API Key metadata、quota/usage 一致；
- ready Artifact 下载 hash 正确；
- Secret 只有在 key ring 同时恢复后才可用；
- terminal Task 没有 active reservation。

## Disaster rehearsal

只在全新、专用 project（例如 `mini-ai-cloud-local-dr-<run-id>`）演练。不可对日常开发栈或未知 volume 执行删除。

1. 启动专用栈，创建并完成带唯一 marker 的 task/artifact。
2. 停 API/Worker/MinIO，运行 `backup.sh`。
3. `docker compose down` 后，通过 Compose labels 只读确认 PostgreSQL target volume 恰好一个，名称和 project label 都匹配。
4. 再次人工确认 run id 后，只删除该精确 volume；不使用 glob、`$HOME`、workspace root 或跨 shell 拼接目标。
5. 运行 `restore.sh --local-stack --confirm-overwrite`。
6. 启动栈并按 marker 查询 task、timeline、logs 和 artifact checksum。
7. 最后删除整个专用 DR project；保留报告与 backup checksum，不能保留 credential。

具体命令和验收项见 [演示手册 Demo 7](demos.md)。当前仓库提供安全的 backup/restore primitives；没有实际执行过专用 volume 删除与恢复时，最终报告必须写“未做真实破坏性 DR 演练”。

## Observability

```bash
make observability
docker compose --profile observability ps
curl -fsS http://localhost:8000/metrics
```

Prometheus/Grafana 本地 profile 用于查看 API latency、task queue/run、scheduler attempts、outbox lag、worker allocation、service replicas 与 gateway 指标。生产环境还应配置：

- outbox oldest age、queued age、offline worker、stuck task 告警；
- CPU/RAM/GPU utilization 与 GPU fragmentation；
- API 5xx/429、gateway upstream error 与 p95/p99 latency；
- PostgreSQL connections/locks/disk、Redis memory/persistence、object-store capacity；
- structured log retention 与 PII/secret scrubbing；
- 告警路由、值班和 runbook 链接。

## Retention 与 cleanup

配置合同包括 Task、Log、Audit、Artifact retention。`CleanupController` 以小批量清理过期 API Key、旧日志/已处理 outbox/audit、可删除的 terminal task runtime metadata、Redis log stream，以及未被 Task/Dataset 引用的旧临时 Artifact；对象删除失败会保留可重试状态。Worker 的 Docker orphan reconciliation 是另一条运行时清理路径。Kubernetes orphan、更多外部对象和长期大表性能仍需现场验证，不能因为配置存在就忽略容量监控。

Redis ready/log/event stream 有独立 length/TTL 上限，但 Redis TTL 不是 PostgreSQL 或 Object Store 的 retention 替代品。

## Kubernetes / Kind

```bash
make kind-up
make test-k8s
make kind-down
```

`kind-up` 创建专用 cluster name，`kind-down` 只删除该 cluster。测试前后记录 `kubectl config current-context`；不要覆盖或永久切换用户已有 context。真实 E2E 还应验证 Pod labels、资源 limits、GPU resource request、logs、cancel、API 重启后的 reconciliation 与 orphan cleanup。

Kubernetes model serving 使用独立命令和 cluster，不与 batch Kind 验证混用：

```bash
make kind-serving-up
make test-kind-serving
make kind-serving-down
```

脚本把 kubeconfig 写入 `build/kind-serving/kubeconfig`，所有 kubectl 和 kind 操作都显式传这个文件。`kind-serving-down` 只删除 `mini-ai-cloud-serving-v4a`，不会删除默认 context 指向的其他集群。若前置条件缺失，命令会打印 `NOT RUN` 并返回非零；先安装或自行提供 Docker、Kind、kubectl，再重跑，不要把这类结果记为 PASS。

常用排障命令：

```bash
KUBECONFIG=build/kind-serving/kubeconfig kubectl -n mini-ai-cloud-serving get pods,svc
KUBECONFIG=build/kind-serving/kubeconfig kubectl -n mini-ai-cloud-serving describe pod <pod>
KUBECONFIG=build/kind-serving/kubeconfig kubectl -n mini-ai-cloud-serving logs deploy/mini-ai-cloud-api
curl -fsS http://127.0.0.1:18080/readyz
```

Controller rollout 时不要手工删除 serving Pod。正常 API Deployment restart 会在 startup recovery 中 adopt 现有 execution；Pod UID 或数量发生变化时，先查 worker session、Pod labels、Replica lease 和 controller 日志。手工删除 Pod 是受控故障测试，预期旧 Replica 进入 lost，随后出现带新 execution 的 replacement。

## 多节点 Worker 边界

每个 Worker 必须有唯一 `WORKER_ID`/hostname/node name，声明 `WORKER_RUNTIME_TYPES`、总/可分配 CPU/RAM、GPU device inventory、labels 与 taints。固定 ID 重注册不能清零旧 lease reservation。

当前 shared token 只是一条过渡边界。跨主机部署前至少：

- 控制平面和 Worker 使用私网或受限 overlay network；
- PostgreSQL/Redis 不暴露给 workload network；
- Worker 只拥有所需 runtime 权限；
- 设计独立节点证书、短期 credential、rotation/revocation 与 mTLS；
- 将 Docker socket 节点视作高权限机器，隔离不同信任等级 workload。

## 排障速查

| 现象 | 先查 | 正确边界/动作 |
| --- | --- | --- |
| `/livez=200`, `/readyz=503` | API 日志、PostgreSQL、Redis | 不接流量；恢复依赖，不能绕过 DB 真相 |
| Task 长期 queued | `task explain`、admin doctor、quota、online worker/GPU | 看 unschedulable reason、policy、labels、fairness 与 reservation |
| Outbox lag 增长 | `/metrics`、admin diagnostics、Redis、dispatcher error | PostgreSQL event 仍在；恢复 dispatcher，避免手改 processed_at |
| Worker offline | heartbeat、lease、host/runtime | Reaper fencing 旧 execution；不要人工把旧 result 标成功 |
| terminal Task 占资源 | admin doctor、resource_reservations | 先只读确认；repair 必须保守且幂等 |
| Service degraded | replica health/lease、fake/vLLM process、gateway error | 不健康 replica 摘除；reconciler 应补齐 desired state |
| Artifact 409/422 | state、declared size/hash、quota、backend | 不绕过 finalize；重新创建 staging record |
| Secret 503 | `SECRET_MASTER_KEY` 是否透传/格式正确 | 不把 key 或 plaintext 打进日志 |
| 429 | API key minute limit | 等 `Retry-After`，不要用多 key 绕 quota/limit |
| Redis 重启 | persistence、consumer group、fallback | 允许通知延迟；任务真相以 PostgreSQL 为准 |
| PostgreSQL 卡顿 | locks/connections/disk/query plan | 停止安全写入；不要 fail open 接收无法持久化的任务 |
| 磁盘增长 | DB logs/events、Redis、Artifact/MinIO、Prometheus | 先找引用与 retention gap，再做受控小批量清理 |

### 通用采集命令

```bash
docker compose ps
docker compose logs --tail=200 api worker migrate postgres redis
curl -fsS http://localhost:8000/metrics
uv run mini-cloud admin doctor
docker ps -a --filter label=mini-ai-cloud.managed=true
```

不要在故障报告中复制 API Key、Secret、cookie、数据库密码、presigned URL 或完整环境变量。
