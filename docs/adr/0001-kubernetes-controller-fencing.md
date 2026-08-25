# ADR 0001：Kubernetes Controller 使用 PostgreSQL Worker 行锁做所有权 fencing

- 状态：已接受
- 日期：2026-08-25
- 范围：Phase IV-A Kubernetes Fake model serving

## 背景

Kubernetes serving controller 使用稳定的 virtual Worker ID。每次进程启动都会生成新的 `worker_session_id`，并通过 `WorkerRepository.register()` 把当前 session 写入 PostgreSQL。PostgreSQL 保存 controller 所有权、Replica generation、`execution_id`、lease 和服务状态。Kubernetes label 用于识别和恢复具体资源，不授予 controller 所有权。

Controller rollout 时，新旧进程可能短暂重叠。旧进程如果在 session 被接管后继续创建或删除 Kubernetes 资源，可能删掉新进程已经接管的 Pod，或者为失效 execution 创建重复资源。仅在写回数据库时检查 generation 和 `execution_id`，无法阻止已经发往 Kubernetes API 的副作用。

## 决策

Phase IV-A 用 `Worker` 行作为同一 Kubernetes serving controller 的所有权 fence。下列外部操作都在同一个 PostgreSQL 事务内执行：

| 操作 | 事务内检查 | Kubernetes 调用 |
| --- | --- | --- |
| 创建资源 | `SELECT Worker FOR UPDATE`，核对 `worker_session_id` | `prepare()`、`start()` |
| graceful stop | `SELECT Worker FOR UPDATE`，核对 `worker_session_id` | `request_stop()` |
| force cleanup | `SELECT Worker FOR UPDATE`，核对 `worker_session_id` | `force_cleanup()` |

`WorkerRepository.register()` 也会锁定同一行。旧 controller 持锁执行 Kubernetes 调用时，新 controller 的注册必须等待。新 session 已经提交后，旧 controller 再进入上述路径会在调用 Kubernetes API 前返回。Replica 的 claim、lease、loading、running 和 terminal 写入还会核对 Worker session、generation 和 `execution_id`。

Kubernetes 资源身份仍要单独校验。当前 runtime 用受管 label、spec hash 和 Pod UID precondition 防止同名资源被错误 adopt 或删除。数据库 fence 与资源身份检查处理的是两个不同问题，前者判断谁可以操作，后者判断操作对象是否仍是预期对象。

## 当前保证

- 同一 stable Worker ID 的 session 接管与受保护的 Kubernetes 写操作不能同时越过 Worker 行锁。
- 新 session 提交后，旧 session 不能开始受保护的 create、stop 或 cleanup，也不能提交带旧 session 的 Replica 状态变更。
- Kubernetes 调用抛错或协程被取消时，事务上下文会回滚。代码离开事务后，Worker 行锁可以被接管方获得。
- generation、`execution_id`、资源 label 和 Pod UID precondition 共同限制 stale result 和 stale handle 的影响范围。

这些保证依赖所有资源变更继续经过受保护的 controller 路径。直接使用集群管理员权限修改资源不在该边界内。

## 风险和未保证的行为

PostgreSQL 事务会覆盖 Kubernetes 网络 I/O。API server 变慢时，数据库连接和 Worker 行锁会被长时间占用，新 controller 注册、同一 Worker 下的其他操作和故障接管也会等待。多个 launch 虽然可以由 controller 并发调度，Worker 行锁仍会把关键外部写操作串行化。

当前设计不提供以下保证：

- PostgreSQL 与 Kubernetes 之间不存在分布式事务，因此不保证跨系统原子提交或 exactly-once。
- 客户端在请求发出后被取消或断连时，远端操作可能已经生效。数据库回滚不能撤销该副作用，后续 recovery 必须重新观察并收敛。
- 如果 Kubernetes 调用一直不返回，也没有触发上层 timeout 或 cancellation，行锁等待时间没有固定上限。
- 进程崩溃、网络分区和数据库 failover 的恢复时间取决于连接回收、客户端 timeout 和基础设施状态，本阶段没有 failover latency SLO。
- 该 fence 不解决 Kubernetes API 不可用、RBAC 配置错误、集群管理员绕过 controller 修改资源，或多集群所有权协调。

## Phase IV-A 接受这一取舍的原因

Phase IV-A 只验证单个 Kubernetes serving cluster 中的 Fake inference 生命周期，项目定位是 production-minded experimental system。当前优先级是阻止 rollout 重叠期间的 stale create 和 stale delete。持有 Worker 行锁的实现直接复用现有 session fencing，改动范围小，也容易通过真实 PostgreSQL 并发测试观察。

把外部 I/O 移出事务需要新增持久化 operation intent、幂等重试、未知结果恢复和 intent 清理。本阶段没有足够的运行规模或延迟证据证明这套状态机的复杂度合理，因此暂不引入。这个决定不把长事务视为生产环境的最终方案。

## 后续候选设计

候选方案把一次操作拆成两个短事务，中间只执行幂等的 Kubernetes 调用：

```text
transaction A:
  lock Worker and Replica
  compare current worker session, generation and execution
  persist operation intent with a unique operation id and expected fences
  commit

external operation:
  execute an idempotent Kubernetes create, stop or delete
  use deterministic identity and delete preconditions

transaction B:
  lock operation intent, Worker and Replica
  compare the same ownership fences
  commit the observed result with CAS
  otherwise mark the intent superseded and discard the stale result
```

Recovery 需要扫描未完成 intent，查询 Kubernetes 当前状态，再安全重试或结束 intent。这个方案缩短数据库行锁持有时间，但会新增 unknown outcome、重复投递、intent retention 和 reaper 并发等状态。

## 迁移触发条件

出现以下任一信号时，应重新评估两事务方案：

- Kubernetes API 延迟让 Worker 行锁等待或数据库连接占用超过明确的服务目标。
- Controller rollout 或故障接管时间经常被外部调用阻塞。
- 增加 controller 副本、多个 serving cluster，或真实 GPU 模型加载后，单 Worker 串行化限制了吞吐。
- 监控持续发现 long transaction、lock timeout、连接池耗尽或死锁重试。
- 取消和网络中断经常产生未知结果，需要跨进程持久化重试与审计。
- 业务要求独立控制数据库事务 timeout 和 Kubernetes operation timeout。

迁移前必须先定义 Kubernetes 操作的幂等键、intent 状态机、CAS 条件、未知结果恢复、retention 和可观测性，不能只把现有网络调用移到事务外。

## 验证边界

`tests/integration/test_kubernetes_serving_postgresql.py` 中的 targeted cases 使用 `LIVE_DATABASE_URL` 指向真实 PostgreSQL，覆盖 concurrent claim、`SKIP LOCKED`、session takeover 后拒绝 stale Replica mutation，以及 Kubernetes cleanup 阻塞后取消、释放行锁、完成接管并拒绝旧 session 删除。SQLite 测试不作为 `FOR UPDATE` 和 `SKIP LOCKED` 的证据。

测试文件存在不等于测试已经通过。具体 CI run 和 Kind 验收结果以当前 verification report 与 GitHub Actions 为准。
