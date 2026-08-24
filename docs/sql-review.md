# SQL Hot-path Review

测量时间：2026-08-23。后端为本机 Compose PostgreSQL 16；样本包含 141 Tasks（0 queued）、0 dependency edges、15 Workers、429 Outbox events、1 Resource reservation、0 Usage ledger rows。以下是实际 `EXPLAIN (ANALYZE, BUFFERS)`，不是生产容量结论。

## 结果摘要

| Path | Snapshot execution | Plan | 结论与限制 |
| --- | ---: | --- | --- |
| Project task list, limit 100 | 1.386 ms | Seq Scan + 34 kB quicksort | 141 行全部属于同一 Project，索引选择性为零；存在 `(project_id, created_at, id)` index，但此小表 planner 合理选择 seq scan |
| Scheduler queued candidate, limit 128 | 0.146 ms | `ix_tasks_status` → Nested Loop Anti Join + 25 kB sort | 已重测当前 `NOT EXISTS` 形状；queued/edges 都为 0，anti-join 内层未执行，不能外推大队列 p95 |
| Online worker inventory | 0.289 ms | 15-row Seq Scan + sort | 仅 1 online/15 total，小表 seq scan 合理；真实 1000-node inventory 仍需 profile |
| Reaper expired lease | 0.144 ms | `ix_tasks_lease_expires_at` Index Scan | 命中 1 running row；状态过滤在 index cond 后执行 |
| Outbox pending claim | 2.632 ms | `ix_outbox_unprocessed_available` Index Scan + sort | 测量时没有 pending event；9 shared blocks 为 read，不能外推 steady-state latency |
| Usage day aggregate | 0.069 ms | Project index + Aggregate | ledger 为空；`(project_id, finished_at)` composite index 存在，但空表无法验证范围选择性 |

Planning time 在约 1.6–7.343 ms，部分高于 execution time，符合小表/一次性 prepared-less `psql` 测量特征。不能把这组单次结果当 p50/p95/p99，也不能声称已证明 10,000 queued task 的数据库性能。

## 实际查询形状

### Task list

```sql
SELECT id, status, created_at
FROM tasks
WHERE project_id = :project_id
ORDER BY created_at DESC, id DESC
LIMIT 100;
```

对应 index：`ix_tasks_project_created (project_id, created_at, id)`。Task list 已支持不透明 keyset cursor，并保留 offset 兼容路径。当前查询按两个时间键 DESC；PostgreSQL 可反向扫描整个 suffix，但仍需真实多 Project、深页 cursor 数据证明 index path。

### Scheduler candidate

```sql
SELECT id, project_id, priority, queue_order, queued_at
FROM tasks
WHERE status = 'queued' AND cancel_requested IS FALSE
  AND NOT EXISTS (
    SELECT 1
    FROM task_dependencies d
    JOIN tasks dependency ON dependency.id = d.depends_on_task_id
    WHERE d.task_id = tasks.id AND dependency.status <> 'succeeded'
  )
ORDER BY priority DESC, queue_order, queued_at, id
LIMIT 128
FOR UPDATE SKIP LOCKED;
```

初次审计发现 scheduler 在取回最多 128 个 Task 后逐个调用 `dependencies_ready(task.id)`。当前 candidate query 已把 dependency gate 合并为 correlated `NOT EXISTS`，仍保留 `FOR UPDATE SKIP LOCKED`；修复后的本机计划实际为 `Index Scan(ix_tasks_status) → Nested Loop Anti Join → Sort → LockRows`，execution 0.146 ms、shared hit 12/read 1。不过样本没有 queued task 或 dependency edge，anti-join 内层显示 `never executed`，只能证明 SQL 可执行和计划形状，不能证明有依赖大队列的性能。`tests/unit/test_scheduler_queries.py` 固定 SQL 形状，避免退回逐候选查询。最终 `place()` 对被选中的单个 task 再做一次 fenced readiness re-check，这是并发安全校验，不是按候选放大的 N+1。

`resolve_dependency_readiness` 也已改为：先 fenced 领取本轮 dependent tasks，再用一个 `task_id IN (...)` 查询批量读取全部 dependency rows并在内存分组；发生状态变化时整批复用一次 PostgreSQL clock。`tests/integration/test_dependency_readiness_batching.py` 用 20 个 waiting tasks 约束等待分支恒为 2 条 SELECT（task batch + dependency batch），并用 20 个 all-ready tasks 约束恒为 3 条 SELECT（再加一次 DB clock）、整批全部晋升且各生成 ready outbox，避免 edge query、clock query 或漏处理候选回归。仍需有真实 10k-edge 数据的 statement/latency benchmark。

### Worker inventory

Worker、GPUDevice 与 active ReservationGPUDevice 分三次批量读取，没有逐 Worker query。1000-node 模拟不是 SQL inventory benchmark；应在 PostgreSQL 填充受控 inventory 后测量总 round trips、bytes 和 materialization 时间。

### Reaper

```sql
SELECT ...
FROM tasks
WHERE status IN (:active_statuses)
  AND lease_expires_at < now()
ORDER BY lease_expires_at
LIMIT :batch
FOR UPDATE SKIP LOCKED;
```

`ix_tasks_lease_expires_at` 被实际使用。后续可以考虑 active-status partial index，但只有生产风格分布和写放大测量支持时才值得增加。

### Outbox

```sql
SELECT ...
FROM outbox_events
WHERE processed_at IS NULL
  AND available_at <= now()
  AND (locked_until IS NULL OR locked_until < now())
ORDER BY created_at
LIMIT :batch
FOR UPDATE SKIP LOCKED;
```

partial index `ix_outbox_unprocessed_available` 被使用。当前 plan 仍按 `created_at` 排序；当 pending backlog 很大时，应测量 `(available_at, created_at)` 与公平性/重试语义的取舍，不能只为消除 sort 改顺序。

Project event/WebSocket 恢复路径独立按 `(created_at, id)` 做 keyset polling，schema migration `0009_outbox_event_cursor` 为该顺序增加 `ix_outbox_events_created_id`。它能避免无界排序，但项目归属仍是跨 Task/Service/Artifact/Dataset/JobGroup 的 project-scoped `OR`/subquery 谓词；大 backlog、多 Project 下应继续测量计划，不能把“有索引”写成“查询一定走最优计划”。

### Job Groups

维护性/性能复审用 3 个 group 实测列表序列化：修复前逐 group 查询 dependencies/tasks，共 11 条 `SELECT`；`DAGRepository.summarize_groups()` 改为对当前页 groups 各批量读取一次 edges 和 tasks 后，固定为 4 条 `SELECT`（group list、count、edges、tasks）。回归测试约束 query count 不随当前页 group 数增长；尚未用 10k groups/edges 测量 materialization 与 payload latency。

### Usage

```sql
SELECT count(id), sum(cpu_seconds), sum(memory_gb_seconds), sum(gpu_seconds)
FROM usage_ledger
WHERE project_id = :project_id
  AND finished_at >= :from
  AND finished_at < :to;
```

schema 有 `ix_usage_project_period (project_id, finished_at)`。本次 0-row snapshot 无法验证 aggregation cost；应生成多 Project、至少百万 ledger rows 的只读 benchmark，再决定是否需要 rollup/materialized view。

## 下一轮性能证据

1. 创建专用 benchmark DB：100 Projects、100 Workers、10,000 queued Tasks、混合 dependency/GPU，保留 deterministic seed。
2. 对有 10k edges 的 candidate anti-join 与 readiness batch 分别记录 statements、latency、rows 和 buffers。
3. 分别记录 warm/cold cache、5 次以上重复测量和 `pg_stat_statements` mean/p95 proxy。
4. Task/Worker/Service/Artifact cursor list 测第一页和深页；offset 仅做兼容对照。
5. Outbox backlog 0/10k/1m 与 10% retrying 三种分布。
6. Usage 1m/10m ledger rows、多 currency/GPU model aggregation。
7. 记录 PostgreSQL CPU、shared hit/read、temp spill、lock wait 与 round trips；报告机器规格和 commit。

Scheduler simulator 的 placements/sec/latency 是算法模拟证据，不等于这些 PostgreSQL query 的性能证据，最终报告必须分开。
