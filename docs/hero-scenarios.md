# Hero scenarios

三个统一入口复用仓库现有的 fencing integration test 与真实 Kind serving E2E，
不维护第二套系统行为实现，也不要求真实 GPU：

```bash
uv sync --frozen --all-groups
uv run mini-cloud demo fencing
uv run mini-cloud demo controller-adoption
uv run mini-cloud demo sse-drain
uv run mini-cloud demo all
```

`fencing` 使用隔离的 SQLite test database，验证 worker session takeover 后旧 session
不能续租、写日志、读取 Secret、发布 artifact 或提交 terminal result。

`controller-adoption` 与 `sse-drain` 复用 `scripts/kind_serving_e2e.py` 的完整真实
Kubernetes 生命周期。它们需要 Linux Docker Engine、Buildx、Kind、kubectl、make 和
uv，但只使用 fake inference Pod，不将结果表述为真实 GPU/vLLM 性能。

每次运行会输出目标、前置检查、执行步骤、关键 identity/status 变化、PASS/FAIL、
cleanup 状态和 `build/hero-demo/<run-id>/summary.json`。原始命令输出写入同目录的
`logs/`，常见 credential 形式会再次脱敏。失败时先收集已有 redacted Kind diagnostics，
再执行 `make kind-serving-down`；成功也执行相同 cleanup。

为避免误删用户已有环境，runner 在启动前若发现专用 cluster
`mini-ai-cloud-serving-v4a` 已存在，会 fail closed，不会接管或删除它。请先人工确认该
cluster 属于本项目，再运行 `make kind-serving-down`。一次 `demo all` 只建立并测试一轮
Kind stack，同时验证 adoption 和 SSE drain；第二次运行使用新的 artifact 目录，且不会
依赖第一次的残留资源。
