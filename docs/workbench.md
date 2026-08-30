# Web Workbench

Web Workbench 是 Mini AI Cloud 自带的轻量控制面界面。它直接调用现有 REST API，用于查看系统健康、任务生命周期、模型服务副本、Worker 容量以及 Project 用量和配额，也提供少量常用操作。

## 启动

```bash
docker compose up --build -d
```

浏览器打开：

```text
http://localhost:8000/workbench
```

Workbench 由 FastAPI 同源托管，不需要 Node、npm、CDN 或单独的前端服务。源码运行、Docker Compose 和 wheel 安装使用同一组静态资源。

## 认证

Workbench 只连接托管当前页面的同源 Mini AI Cloud API。连接页会显示并锁定当前 API origin，不支持填写远程 API Base URL。输入 Project API Key 后，Workbench 会调用：

- `GET /api/v1/auth/whoami`
- `GET /api/v1/projects/current`

验证成功后，API Key 仅保存在当前浏览器 tab 的 `sessionStorage` 中。它不会进入 URL、`localStorage`、页面日志或浏览器控制台。点击 `Disconnect / Forget` 会清除当前会话中的 key。

## 页面

- Overview 汇总 `/livez`、`/readyz`、`/health`，并显示 Task、Service、Worker 和最近 24 小时用量。
- Tasks 提供状态筛选、任务详情、真实时间轨迹、调度解释、日志、取消操作和 Quick Run。
- Services 展示 desired、actual、healthy replicas，提供 Replica 明细、Scale、Stop 和 Quick Deploy。
- Workers 展示 Worker inventory 与 reservation，包括 slots、CPU、内存和 accelerator 容量。
- Usage 按 1h、24h、7d、30d 窗口展示 usage、serving usage、cost 和 Project quota。
- System 显示控制面健康、应用版本，并提供 OpenAPI 和 Prometheus 入口。

页面可见时，Overview、Services 和 Workers 默认约每 5 秒刷新；活跃 Task detail 约每 2.5 秒刷新。页面隐藏时会暂停轮询，新的刷新会取消尚未完成的旧请求。

## 安全边界

Workbench 不增加认证机制，也不绕过 RBAC、quota、image policy、admission 或 scheduler。所有操作仍由现有 API 做权限检查和业务校验。Task environment values 始终以 `MASKED` 显示，结构化 API 错误会保留 code、message、details 和 request ID，Cancel、Scale、Stop 都需要确认。

动态 API 数据通过 DOM `textContent` 和节点 API 渲染。静态页面禁止外部脚本、样式、对象资源和跨 origin API 连接，并使用 `no-store`，避免浏览器长期缓存连接页面。

## 已知限制

- Workbench 是 development/operator convenience UI，不是 production-grade 多租户控制台。
- 它没有用户、成员、API Key、Dataset、Artifact 或 Registry 管理页面。
- Worker 容量来自 Mini AI Cloud inventory 与 reservation，不是实时硬件 telemetry。
- Usage API 目前返回聚合窗口，Workbench 不伪造时间序列图。
- Prometheus 和 Grafana 继续承担专业监控、趋势与告警职责。
