(() => {
  "use strict";

  const SESSION_API_KEY = "mini_ai_cloud_workbench_key";
  const LIST_PAGE_SIZE = 100;
  const TASK_LOG_PAGE_SIZE = 500;
  const TERMINAL_TASK_STATUSES = new Set([
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "preempted",
  ]);
  const PAGE_TITLES = {
    overview: ["Control plane", "Overview"],
    tasks: ["Workloads", "Tasks"],
    services: ["Serving", "Model Services"],
    workers: ["Capacity", "Workers & Nodes"],
    usage: ["Accounting", "Usage & Quota"],
    system: ["Diagnostics", "System"],
  };

  const state = {
    apiBase: window.location.origin,
    apiKey: "",
    principal: null,
    project: null,
    page: "overview",
    autoRefresh: true,
    refreshTimer: null,
    requestControllers: new Map(),
    detail: null,
    detailTerminal: true,
    detailPollCount: 0,
    logsAutoScroll: true,
    taskLogs: {
      taskId: null,
      offset: 0,
      entries: [],
    },
    listPages: {
      tasks: { key: "", cursor: null, history: [] },
      services: { key: "", cursor: null, history: [] },
      workers: { key: "", cursor: null, history: [] },
    },
    lastData: {
      tasks: [],
      services: [],
      workers: [],
    },
  };

  class ApiError extends Error {
    constructor(status, code, message, details, requestId) {
      super(message);
      this.name = "ApiError";
      this.status = status;
      this.code = code;
      this.details = details;
      this.requestId = requestId;
    }
  }

  function query(selector, root = document) {
    return root.querySelector(selector);
  }

  function node(tag, options = {}, children = []) {
    const element = document.createElement(tag);
    if (options.className) element.className = options.className;
    if (options.text !== undefined) element.textContent = String(options.text);
    if (options.id) element.id = options.id;
    if (options.title) element.title = options.title;
    if (options.type) element.type = options.type;
    if (options.href) element.href = options.href;
    if (options.target) element.target = options.target;
    if (options.rel) element.rel = options.rel;
    if (options.value !== undefined) element.value = String(options.value);
    if (options.checked !== undefined) element.checked = Boolean(options.checked);
    if (options.disabled !== undefined) element.disabled = Boolean(options.disabled);
    if (options.colSpan) element.colSpan = options.colSpan;
    if (options.role) element.setAttribute("role", options.role);
    if (options.tabIndex !== undefined) element.tabIndex = options.tabIndex;
    if (options.dataset) {
      for (const [key, value] of Object.entries(options.dataset)) {
        element.dataset[key] = String(value);
      }
    }
    if (options.attrs) {
      for (const [key, value] of Object.entries(options.attrs)) {
        element.setAttribute(key, String(value));
      }
    }
    if (options.on) {
      for (const [eventName, handler] of Object.entries(options.on)) {
        element.addEventListener(eventName, handler);
      }
    }
    appendChildren(element, children);
    return element;
  }

  function appendChildren(parent, children) {
    const values = Array.isArray(children) ? children : [children];
    for (const child of values) {
      if (child === null || child === undefined || child === false) continue;
      parent.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
  }

  function replace(target, children = []) {
    const element = typeof target === "string" ? query(target) : target;
    if (!element) return;
    element.replaceChildren();
    appendChildren(element, children);
    element.classList.remove("loading-block");
  }

  function valueOrDash(value) {
    if (value === null || value === undefined || value === "") return "—";
    if (Array.isArray(value)) return value.length ? value.join(", ") : "—";
    if (typeof value === "object") return JSON.stringify(value);
    if (typeof value === "boolean") return value ? "Yes" : "No";
    return String(value);
  }

  function formatNumber(value, maximumFractionDigits = 1) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "—";
    return new Intl.NumberFormat(undefined, { maximumFractionDigits }).format(numeric);
  }

  function formatTime(value, includeSeconds = false) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: includeSeconds ? "2-digit" : undefined,
    }).format(date);
  }

  function formatDuration(milliseconds) {
    const numeric = Number(milliseconds);
    if (!Number.isFinite(numeric) || numeric < 0) return "—";
    const seconds = Math.floor(numeric / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ${minutes % 60}m`;
  }

  function taskDuration(task) {
    if (task.duration_ms !== null && task.duration_ms !== undefined) {
      return formatDuration(task.duration_ms);
    }
    if (!task.started_at) return "—";
    const end = task.finished_at ? new Date(task.finished_at) : new Date();
    return formatDuration(end.getTime() - new Date(task.started_at).getTime());
  }

  function shortId(value) {
    if (!value) return "—";
    const text = String(value);
    return text.length > 12 ? text.slice(0, 8) : text;
  }

  function statusTone(status) {
    const normalized = String(status || "").toLowerCase();
    if (["ok", "online", "healthy", "running", "succeeded", "ready"].includes(normalized)) {
      return "good";
    }
    if (
      [
        "pending",
        "queued",
        "scheduling",
        "assigned",
        "preparing",
        "pulling",
        "starting",
        "loading",
        "deploying",
        "retrying",
      ].includes(normalized)
    ) {
      return "active";
    }
    if (["degraded", "draining", "stopping", "preempting", "unknown"].includes(normalized)) {
      return "warn";
    }
    if (
      ["error", "failed", "offline", "unhealthy", "lost", "cancelled", "timed_out"].includes(
        normalized,
      )
    ) {
      return "bad";
    }
    return "neutral";
  }

  function badge(status, label = null) {
    return node("span", {
      className: `status-badge ${statusTone(status)}`,
      text: label || valueOrDash(status),
    });
  }

  function normalizeApiBase(rawValue) {
    const url = new URL(rawValue, window.location.origin);
    if (!["http:", "https:"].includes(url.protocol)) {
      throw new Error("API Base URL must use http or https.");
    }
    if (url.username || url.password) {
      throw new Error("API Base URL must not contain credentials.");
    }
    if (url.origin !== window.location.origin) {
      throw new Error("Workbench API origin must match the page origin.");
    }
    return window.location.origin;
  }

  function createIdempotencyKey() {
    const cryptoApi = window.crypto;
    if (cryptoApi && typeof cryptoApi.randomUUID === "function") {
      return `workbench-${cryptoApi.randomUUID()}`;
    }
    if (cryptoApi && typeof cryptoApi.getRandomValues === "function") {
      const entropy = new Uint32Array(4);
      cryptoApi.getRandomValues(entropy);
      const suffix = Array.from(entropy, (value) => value.toString(16).padStart(8, "0")).join("");
      return `workbench-${suffix}`;
    }
    return `workbench-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  }

  function sessionGet(key) {
    try {
      return window.sessionStorage.getItem(key) || "";
    } catch (_error) {
      return "";
    }
  }

  function sessionSet(key, value) {
    try {
      window.sessionStorage.setItem(key, value);
    } catch (_error) {
      // The workbench remains usable for this page lifetime if storage is unavailable.
    }
  }

  function sessionRemove(key) {
    try {
      window.sessionStorage.removeItem(key);
    } catch (_error) {
      // Nothing else is persisted.
    }
  }

  async function api(path, options = {}) {
    const channel = options.channel || `${options.method || "GET"}:${path}`;
    const previous = state.requestControllers.get(channel);
    if (previous) previous.abort();
    const controller = new AbortController();
    state.requestControllers.set(channel, controller);

    const headers = new Headers(options.headers || {});
    headers.set("Accept", "application/json");
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    if (options.auth !== false && state.apiKey) {
      headers.set("Authorization", `Bearer ${state.apiKey}`);
    }

    let response;
    try {
      response = await fetch(`${state.apiBase}${path}`, {
        method: options.method || "GET",
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: controller.signal,
        credentials: "omit",
        cache: "no-store",
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json") ? await response.json() : null;
      const accepted = new Set(options.acceptStatuses || []);
      if (!response.ok && !accepted.has(response.status)) {
        const error = payload && payload.error ? payload.error : {};
        throw new ApiError(
          response.status,
          error.code || `HTTP_${response.status}`,
          error.message || `Request failed with HTTP ${response.status}`,
          error.details,
          error.request_id || response.headers.get("x-request-id"),
        );
      }
      return payload;
    } catch (error) {
      if (error instanceof ApiError || error.name === "AbortError") throw error;
      throw new ApiError(0, "NETWORK_ERROR", "Unable to reach the API.", null, null);
    } finally {
      if (state.requestControllers.get(channel) === controller) {
        state.requestControllers.delete(channel);
      }
    }
  }

  function abortAllRequests() {
    for (const controller of state.requestControllers.values()) controller.abort();
    state.requestControllers.clear();
  }

  function formatApiError(error) {
    if (error && error.name === "AbortError") return "Request replaced by a newer refresh.";
    if (!(error instanceof ApiError)) return "Unexpected workbench error.";
    const lines = [`${error.code}: ${error.message}`];
    if (error.details !== null && error.details !== undefined) {
      lines.push(formatDetails(error.details));
    }
    if (error.requestId) lines.push(`Request ID: ${error.requestId}`);
    return lines.join("\n");
  }

  function formatDetails(details) {
    if (typeof details === "string") return details;
    try {
      return JSON.stringify(details, null, 2);
    } catch (_error) {
      return String(details);
    }
  }

  function showGlobalError(error) {
    if (error && error.name === "AbortError") return;
    const target = query("#global-error");
    target.textContent = formatApiError(error);
    target.classList.remove("hidden");
  }

  function clearGlobalError() {
    const target = query("#global-error");
    target.textContent = "";
    target.classList.add("hidden");
  }

  function unavailable(error, compact = false) {
    const permissionDenied = error instanceof ApiError && [401, 403, 404].includes(error.status);
    const message = permissionDenied
      ? "Unavailable with current permission"
      : error instanceof ApiError
        ? `${error.code}: ${error.message}`
        : "Data unavailable";
    return node("div", {
      className: compact ? "unavailable" : "empty-state",
      text: message,
    });
  }

  function metricCard(label, value, detail, status = null) {
    return node("article", { className: "metric-card" }, [
      node("div", { className: "metric-topline" }, [
        node("span", { text: label }),
        status ? badge(status) : null,
      ]),
      node("strong", { className: "metric-value", text: value }),
      node("p", { className: "metric-detail", text: detail || "" }),
    ]);
  }

  function progressBar(current, total, tone = "good") {
    const numericCurrent = Number(current) || 0;
    const numericTotal = Number(total) || 0;
    const percent = numericTotal > 0 ? Math.min(100, Math.max(0, (numericCurrent / numericTotal) * 100)) : 0;
    const widthBucket = Math.round(percent / 5) * 5;
    const fill = node("div", { className: `progress-fill ${tone} width-${widthBucket}` });
    return node("div", { className: "progress-track", title: `${formatNumber(percent)}%` }, [fill]);
  }

  function emptyState(title, description, actionLabel = null, action = null, compact = false) {
    return node("div", { className: `empty-state${compact ? " compact" : ""}` }, [
      node("div", {}, [
        node("h3", { text: title }),
        node("p", { text: description }),
        actionLabel && action
          ? node("button", { className: "button compact", type: "button", text: actionLabel, on: { click: action } })
          : null,
      ]),
    ]);
  }

  function dataTable(columns, rows, onRow = null) {
    const table = node("table", { className: "data-table" });
    const headerRow = node("tr");
    for (const column of columns) {
      headerRow.append(node("th", { text: column.label }));
    }
    table.append(node("thead", {}, [headerRow]));
    const body = node("tbody");
    for (const row of rows) {
      const tableRow = node("tr", {
        className: onRow ? "clickable" : "",
        tabIndex: onRow ? 0 : undefined,
      });
      for (const column of columns) {
        const rendered = column.render ? column.render(row) : valueOrDash(row[column.key]);
        tableRow.append(node("td", { className: column.className || "" }, [rendered]));
      }
      if (onRow) {
        tableRow.addEventListener("click", () => onRow(row));
        tableRow.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onRow(row);
          }
        });
      }
      body.append(tableRow);
    }
    table.append(body);
    return node("div", { className: "data-table-wrap" }, [table]);
  }

  function currentListPage(resource, key = "") {
    if (state.listPages[resource].key !== key) {
      state.listPages[resource] = { key, cursor: null, history: [] };
    }
    return state.listPages[resource];
  }

  function resetListPages() {
    for (const resource of Object.keys(state.listPages)) {
      state.listPages[resource] = { key: "", cursor: null, history: [] };
    }
  }

  function listCursorSuffix(page) {
    return page.cursor ? `&cursor=${encodeURIComponent(page.cursor)}` : "";
  }

  function listPagination(resource, pagination, itemCount, refresh) {
    const page = state.listPages[resource];
    const nextCursor = pagination.next_cursor || null;
    const total = Number.isFinite(Number(pagination.total)) ? Number(pagination.total) : itemCount;
    const first = itemCount ? page.history.length * LIST_PAGE_SIZE + 1 : 0;
    const last = first ? first + itemCount - 1 : 0;

    const move = (event, cursor, rememberCurrent) => {
      event.currentTarget.disabled = true;
      if (rememberCurrent) page.history.push(page.cursor);
      else page.history.pop();
      page.cursor = cursor;
      refresh().catch(showGlobalError);
    };
    const previousCursor = page.history.length ? page.history[page.history.length - 1] : null;
    return node("div", { className: "list-pagination" }, [
      node("span", {
        className: "muted",
        text: itemCount
          ? `${first}–${last} of ${formatNumber(total, 0)} · page ${page.history.length + 1}`
          : `No items · ${formatNumber(total, 0)} total · page ${page.history.length + 1}`,
      }),
      node("div", { className: "pagination-actions" }, [
        node("button", {
          className: "button compact ghost",
          type: "button",
          text: "Previous",
          disabled: !page.history.length,
          on: { click: (event) => move(event, previousCursor, false) },
        }),
        node("button", {
          className: "button compact ghost",
          type: "button",
          text: "Next",
          disabled: !nextCursor,
          on: { click: (event) => move(event, nextCursor, true) },
        }),
      ]),
    ]);
  }

  function detailSection(title, content) {
    return node("section", { className: "detail-section" }, [node("h3", { text: title }), content]);
  }

  function detailGrid(record, fields) {
    const list = node("dl", { className: "detail-grid" });
    for (const field of fields) {
      const value = typeof field.value === "function" ? field.value(record) : record[field.value];
      list.append(
        node("div", { className: "detail-item" }, [
          node("dt", { text: field.label }),
          node("dd", { text: valueOrDash(value) }),
        ]),
      );
    }
    return list;
  }

  function keyValueList(record) {
    const list = node("div", { className: "key-value-list" });
    const entries = Object.entries(record || {});
    if (!entries.length) return node("p", { className: "muted", text: "No details recorded." });
    for (const [key, value] of entries) {
      list.append(
        node("div", { className: "key-value-row" }, [
          node("span", { text: key }),
          node("span", { text: valueOrDash(value) }),
        ]),
      );
    }
    return list;
  }

  function setLastUpdated() {
    query("#last-updated").textContent = `Last updated: ${formatTime(new Date().toISOString(), true)}`;
  }

  function setApiState(status) {
    const element = query("#api-state");
    element.className = `status-badge ${statusTone(status)}`;
    element.textContent = status === "ok" ? "Healthy" : valueOrDash(status);
  }

  function openDrawer(type, id, title) {
    state.detail = { type, id };
    state.detailTerminal = false;
    state.detailPollCount = 0;
    resetTaskLogs(type === "task" ? id : null);
    query("#drawer-eyebrow").textContent = `${type} detail`;
    query("#drawer-title").textContent = title;
    replace("#drawer-content", [node("div", { className: "loading-block" })]);
    const drawer = query("#detail-drawer");
    drawer.classList.add("open");
    drawer.setAttribute("aria-hidden", "false");
    query("#drawer-backdrop").classList.remove("hidden");
  }

  function closeDrawer() {
    state.detail = null;
    state.detailTerminal = true;
    state.detailPollCount = 0;
    resetTaskLogs();
    const drawer = query("#detail-drawer");
    drawer.classList.remove("open");
    drawer.setAttribute("aria-hidden", "true");
    query("#drawer-backdrop").classList.add("hidden");
    for (const [channel, controller] of state.requestControllers.entries()) {
      if (channel.startsWith("detail:")) {
        controller.abort();
        state.requestControllers.delete(channel);
      }
    }
    scheduleRefresh();
  }

  function refreshInterval() {
    if (state.detail && state.detail.type === "task" && !state.detailTerminal) return 2500;
    if (["usage", "system"].includes(state.page)) return 30000;
    return 5000;
  }

  function scheduleRefresh(delay = refreshInterval()) {
    if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
    if (!state.autoRefresh || document.hidden || !state.apiKey) return;
    state.refreshTimer = window.setTimeout(() => {
      refreshCurrentPage({ background: true }).catch(showGlobalError);
    }, delay);
  }

  async function refreshCurrentPage(options = {}) {
    if (!state.apiKey || document.hidden) return;
    clearGlobalError();
    try {
      const activeTaskDetail =
        state.detail && state.detail.type === "task" && !state.detailTerminal;
      let skipPageRefresh = false;
      if (options.background && activeTaskDetail) {
        state.detailPollCount += 1;
        skipPageRefresh = state.detailPollCount % 2 === 1;
      }
      if (!skipPageRefresh) {
        if (state.page === "overview") await renderOverview();
        if (state.page === "tasks") await renderTasks();
        if (state.page === "services") await renderServices();
        if (state.page === "workers") await renderWorkers();
        if (state.page === "usage") await renderUsage();
        if (state.page === "system") await renderSystem();
      }
      if (state.detail) await refreshDetail();
      setLastUpdated();
    } catch (error) {
      if (error.name !== "AbortError") showGlobalError(error);
    } finally {
      scheduleRefresh();
    }
  }

  function goToPage(page) {
    if (!PAGE_TITLES[page]) return;
    state.page = page;
    for (const navItem of document.querySelectorAll(".nav-item")) {
      navItem.classList.toggle("active", navItem.dataset.page === page);
    }
    for (const pageElement of document.querySelectorAll(".page")) {
      pageElement.classList.toggle("active", pageElement.id === `page-${page}`);
    }
    query("#page-eyebrow").textContent = PAGE_TITLES[page][0];
    query("#page-title").textContent = PAGE_TITLES[page][1];
    refreshCurrentPage().catch(showGlobalError);
  }

  async function connect(apiBase, apiKey) {
    state.apiBase = normalizeApiBase(apiBase);
    state.apiKey = apiKey;
    const [principal, project] = await Promise.all([
      api("/api/v1/auth/whoami", { channel: "connect:whoami" }),
      api("/api/v1/projects/current", { channel: "connect:project" }),
    ]);
    state.principal = principal;
    state.project = project;
    sessionSet(SESSION_API_KEY, state.apiKey);
    query("#api-key").value = "";
    query("#current-project").textContent = `${project.name} (${project.slug})`;
    query("#connection-view").classList.add("hidden");
    query("#app-view").classList.remove("hidden");
    setApiState("ok");
    await refreshCurrentPage();
  }

  function disconnect() {
    abortAllRequests();
    if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
    sessionRemove(SESSION_API_KEY);
    state.apiKey = "";
    state.principal = null;
    state.project = null;
    resetListPages();
    closeDrawer();
    query("#connection-view").classList.remove("hidden");
    query("#app-view").classList.add("hidden");
    query("#api-base").value = window.location.origin;
    query("#api-key").value = "";
    query("#connection-error").classList.add("hidden");
  }

  async function renderOverview() {
    const now = new Date();
    const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const usagePath = state.project
      ? `/api/v1/projects/${encodeURIComponent(state.project.id)}/usage?from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(now.toISOString())}`
      : null;
    const requests = [
      api("/livez", { auth: false, channel: "overview:live", acceptStatuses: [503] }),
      api("/readyz", { auth: false, channel: "overview:ready", acceptStatuses: [503] }),
      api("/health", { auth: false, channel: "overview:health", acceptStatuses: [503] }),
      api("/api/v1/tasks?limit=1000", { channel: "overview:tasks" }),
      api("/api/v1/services?limit=1000", { channel: "overview:services" }),
      api("/api/v1/workers?limit=1000", { channel: "overview:workers" }),
      usagePath ? api(usagePath, { channel: "overview:usage" }) : Promise.reject(new Error("Project unavailable")),
    ];
    const [liveResult, readyResult, healthResult, tasksResult, servicesResult, workersResult, usageResult] =
      await Promise.allSettled(requests);

    const live = settledValue(liveResult);
    const ready = settledValue(readyResult);
    const health = settledValue(healthResult);
    setApiState(live ? live.status : "error");
    const healthCards = [
      healthMetric("API", live && live.status, liveResult),
      healthMetric("PostgreSQL", health && health.checks && health.checks.postgresql, healthResult),
      healthMetric("Redis", health && health.checks && health.checks.redis, healthResult),
      healthMetric("Control plane readiness", ready && ready.status, readyResult),
    ];
    replace("#overview-health", healthCards);

    const tasks = settledItems(tasksResult);
    const services = settledItems(servicesResult);
    const workers = settledItems(workersResult);
    const tasksPayload = settledValue(tasksResult);
    const servicesPayload = settledValue(servicesResult);
    const workersPayload = settledValue(workersResult);
    const usage = settledValue(usageResult);
    if (tasks) state.lastData.tasks = tasks;
    if (services) state.lastData.services = services;
    if (workers) state.lastData.workers = workers;

    const summaryCards = [
      tasks
        ? taskSummaryCard(tasks, tasksPayload.pagination && tasksPayload.pagination.total)
        : metricCard("Tasks", "Unavailable", "Unavailable with current permission", "unknown"),
      services
        ? serviceSummaryCard(services, servicesPayload.pagination && servicesPayload.pagination.total)
        : metricCard("Services", "Unavailable", "Unavailable with current permission", "unknown"),
      workers
        ? workerSummaryCard(workers, workersPayload.pagination && workersPayload.pagination.total)
        : metricCard("Workers", "Unavailable", "Unavailable with current permission", "unknown"),
      usage
        ? metricCard(
            "Usage · 24h",
            `${formatNumber(usage.execution_count, 0)} executions`,
            `${formatNumber(usage.cpu_seconds)} CPU s · ${formatNumber(usage.gpu_seconds)} accelerator s · ${formatNumber(usage.serving && usage.serving.request_count, 0)} serving requests`,
          )
        : metricCard("Usage · 24h", "Unavailable", "Unavailable with current permission", "unknown"),
    ];
    replace("#overview-summary", summaryCards);
    renderOverviewRecent(tasksResult, servicesResult);
  }

  function settledValue(result) {
    return result && result.status === "fulfilled" ? result.value : null;
  }

  function settledItems(result) {
    const value = settledValue(result);
    return value && Array.isArray(value.items) ? value.items : null;
  }

  function healthMetric(label, status, result) {
    if (result.status === "rejected") {
      return metricCard(label, "Unavailable", "Health endpoint could not be read", "error");
    }
    const normalized = status || "unknown";
    return metricCard(label, normalized === "ok" ? "Operational" : "Degraded", `Reported: ${normalized}`, normalized);
  }

  function countStatuses(items, statuses) {
    const allowed = new Set(statuses);
    return items.filter((item) => allowed.has(item.status)).length;
  }

  function taskSummaryCard(tasks, reportedTotal) {
    const total = Number.isFinite(Number(reportedTotal)) ? Number(reportedTotal) : tasks.length;
    const running = countStatuses(tasks, ["running"]);
    const queued = countStatuses(tasks, ["pending", "queued", "scheduling", "assigned", "preparing", "pulling", "starting", "retrying"]);
    const failed = countStatuses(tasks, ["failed", "timed_out"]);
    const succeeded = countStatuses(tasks, ["succeeded"]);
    return metricCard(
      "Tasks",
      formatNumber(total, 0),
      `${queued} queued · ${running} running · ${failed} failed · ${succeeded} succeeded${total > tasks.length ? " · status counts cover first 1,000" : ""}`,
      failed ? "degraded" : running ? "running" : "ok",
    );
  }

  function serviceSummaryCard(services, reportedTotal) {
    const total = Number.isFinite(Number(reportedTotal)) ? Number(reportedTotal) : services.length;
    const ready = countStatuses(services, ["running"]);
    const degraded = countStatuses(services, ["degraded", "failed", "stopping"]);
    const healthy = services.reduce((total, item) => total + Number(item.healthy_replicas || 0), 0);
    const desired = services.reduce((total, item) => total + Number(item.desired_replicas || 0), 0);
    return metricCard(
      "Services",
      formatNumber(total, 0),
      `${ready} ready · ${degraded} degraded/stopping · ${healthy}/${desired} healthy replicas${total > services.length ? " · counts cover first 1,000" : ""}`,
      degraded ? "degraded" : ready ? "running" : "ok",
    );
  }

  function workerSummaryCard(workers, reportedTotal) {
    const total = Number.isFinite(Number(reportedTotal)) ? Number(reportedTotal) : workers.length;
    const active = countStatuses(workers, ["online"]);
    const unavailableCount = countStatuses(workers, ["draining", "offline"]);
    const accelerators = workers.reduce((total, worker) => total + Number(worker.gpu_count || 0), 0);
    return metricCard(
      "Workers",
      formatNumber(total, 0),
      `${active} active · ${unavailableCount} draining/offline · ${accelerators} accelerator devices${total > workers.length ? " · counts cover first 1,000" : ""}`,
      unavailableCount ? "degraded" : active ? "online" : "unknown",
    );
  }

  function renderOverviewRecent(tasksResult, servicesResult) {
    const tasks = settledItems(tasksResult);
    if (!tasks) {
      replace("#overview-tasks", [unavailable(tasksResult.reason, true)]);
    } else if (!tasks.length) {
      replace("#overview-tasks", [
        emptyState("No tasks yet", "Submit a small workload and watch it move through the control plane.", "Run your first task", openRunTask, true),
      ]);
    } else {
      const rows = node("div", { className: "resource-list" });
      for (const task of tasks.slice(0, 6)) {
        rows.append(
          node("div", { className: "resource-row", tabIndex: 0, on: { click: () => showTask(task) } }, [
            node("div", {}, [
              node("strong", { className: "short-id", text: shortId(task.id) }),
              node("small", { text: `${task.image} · ${formatTime(task.created_at)}` }),
            ]),
            badge(task.status),
          ]),
        );
      }
      replace("#overview-tasks", [rows]);
    }

    const services = settledItems(servicesResult);
    if (!services) {
      replace("#overview-services", [unavailable(servicesResult.reason, true)]);
    } else if (!services.length) {
      replace("#overview-services", [
        emptyState("No services yet", "Deploy a model endpoint using the existing serving admission path.", "Deploy a service", openDeployService, true),
      ]);
    } else {
      const rows = node("div", { className: "resource-list" });
      for (const service of services.slice(0, 6)) {
        rows.append(
          node("div", { className: "resource-row", tabIndex: 0, on: { click: () => showService(service) } }, [
            node("div", {}, [
              node("strong", { text: service.name }),
              node("small", { text: `${service.healthy_replicas}/${service.actual_replicas}/${service.desired_replicas} healthy/actual/desired` }),
            ]),
            badge(service.status),
          ]),
        );
      }
      replace("#overview-services", [rows]);
    }
  }

  async function renderTasks() {
    const filter = query("#task-status-filter").value;
    const page = currentListPage("tasks", filter);
    const filterSuffix = filter ? `&status=${encodeURIComponent(filter)}` : "";
    const cursorSuffix = listCursorSuffix(page);
    try {
      const payload = await api(
        `/api/v1/tasks?limit=${LIST_PAGE_SIZE}${filterSuffix}${cursorSuffix}`,
        { channel: "page:tasks" },
      );
      const tasks = payload.items || [];
      state.lastData.tasks = tasks;
      if (!tasks.length) {
        replace("#tasks-content", [
          node("div", { className: "content-stack" }, [
            emptyState(
              filter ? "No tasks match this status" : "No tasks yet",
              filter ? "Choose another status or submit a new workload." : "Submit a small workload and follow its real scheduler state.",
              "Run your first task",
              openRunTask,
            ),
            page.history.length
              ? listPagination("tasks", payload.pagination || {}, 0, renderTasks)
              : null,
          ]),
        ]);
        return;
      }
      const columns = [
        { label: "ID", render: (task) => node("span", { className: "short-id", text: shortId(task.id), title: task.id }) },
        { label: "Status", render: (task) => badge(task.status) },
        { label: "Image", render: (task) => node("span", { className: "truncate mono", text: task.image, title: task.image }) },
        { label: "Runtime", render: (task) => `${task.runtime_type} / ${task.workload_type}` },
        { label: "Worker", render: (task) => node("span", { className: "mono", text: shortId(task.worker_id), title: task.worker_id || "" }) },
        { label: "CPU", render: (task) => `${formatNumber(task.cpu_limit)} cores` },
        { label: "Memory", render: (task) => `${formatNumber(task.memory_limit_mb, 0)} MB` },
        { label: "Accelerator", render: taskAcceleratorLabel },
        { label: "Created", render: (task) => formatTime(task.created_at) },
        { label: "Started", render: (task) => formatTime(task.started_at) },
        { label: "Duration", render: taskDuration },
      ];
      replace("#tasks-content", [
        node("div", { className: "content-stack" }, [
          dataTable(columns, tasks, showTask),
          listPagination("tasks", payload.pagination || {}, tasks.length, renderTasks),
        ]),
      ]);
    } catch (error) {
      if (error.name === "AbortError") return;
      replace("#tasks-content", [unavailable(error)]);
    }
  }

  function taskAcceleratorLabel(task) {
    const count = Number(task.gpu_count || 0);
    if (!count) return "CPU";
    const vendor = task.selected_vendor || (task.accelerator_request_json && "requested") || "accelerator";
    const model = task.selected_model || task.gpu_model || "any model";
    return `${count} × ${vendor} ${model}`;
  }

  function showTask(task) {
    openDrawer("task", task.id, `Task ${shortId(task.id)}`);
    refreshTaskDetail().catch(showGlobalError);
  }

  function resetTaskLogs(taskId = null) {
    state.taskLogs = {
      taskId,
      offset: 0,
      entries: [],
    };
  }

  async function fetchTaskLogs(taskId) {
    if (state.taskLogs.taskId !== taskId) resetTaskLogs(taskId);

    while (true) {
      const offset = state.taskLogs.offset;
      const page = await api(
        `/api/v1/tasks/${encodeURIComponent(taskId)}/logs?limit=${TASK_LOG_PAGE_SIZE}&offset=${offset}`,
        { channel: "detail:task:logs" },
      );
      if (state.taskLogs.taskId !== taskId) {
        const error = new Error("Task detail changed while logs were loading.");
        error.name = "AbortError";
        throw error;
      }

      const logs = page.logs || [];
      if (!logs.length) break;
      const nextOffset = Number(logs[logs.length - 1].sequence);
      if (!Number.isInteger(nextOffset) || nextOffset <= offset) {
        throw new ApiError(0, "INVALID_LOG_PAGE", "Task log sequence did not advance.", null, null);
      }
      state.taskLogs.entries.push(...logs);
      state.taskLogs.offset = nextOffset;
      if (logs.length < TASK_LOG_PAGE_SIZE) break;
    }

    return { logs: [...state.taskLogs.entries] };
  }

  async function refreshTaskDetail() {
    if (!state.detail || state.detail.type !== "task") return;
    const taskId = state.detail.id;
    const id = encodeURIComponent(taskId);
    const results = await Promise.allSettled([
      api(`/api/v1/tasks/${id}`, { channel: "detail:task" }),
      api(`/api/v1/tasks/${id}/timeline`, { channel: "detail:task:timeline" }),
      api(`/api/v1/tasks/${id}/scheduling`, { channel: "detail:task:scheduling" }),
      fetchTaskLogs(taskId),
    ]);
    if (!state.detail || state.detail.type !== "task" || state.detail.id !== taskId) return;
    const task = settledValue(results[0]);
    if (!task) {
      replace("#drawer-content", [unavailable(results[0].reason)]);
      state.detailTerminal = true;
      return;
    }
    state.detailTerminal = TERMINAL_TASK_STATUSES.has(task.status);
    query("#drawer-title").textContent = `Task ${shortId(task.id)}`;
    const sections = [
      renderTaskHeader(task),
      renderTaskProgress(task),
      detailSection(
        "Timing",
        detailGrid(task, [
          { label: "Created", value: (item) => formatTime(item.created_at, true) },
          { label: "Queued", value: (item) => formatTime(item.queued_at, true) },
          { label: "Assigned", value: (item) => formatTime(item.assigned_at, true) },
          { label: "Started", value: (item) => formatTime(item.started_at, true) },
          { label: "Finished", value: (item) => formatTime(item.finished_at, true) },
          { label: "Duration", value: taskDuration },
        ]),
      ),
      renderTaskParameters(task),
      renderTaskScheduling(results[2]),
      renderTaskTimeline(results[1]),
      renderTaskLogs(results[3]),
    ];
    replace("#drawer-content", sections);
    if (state.logsAutoScroll) {
      const viewer = query("#task-log-viewer");
      if (viewer) viewer.scrollTop = viewer.scrollHeight;
    }
  }

  function renderTaskHeader(task) {
    const actions = node("div", { className: "section-heading" }, [
      node("div", {}, [badge(task.status), node("p", { className: "metric-detail mono", text: task.id })]),
      !TERMINAL_TASK_STATUSES.has(task.status)
        ? node("button", {
            className: "button danger compact",
            type: "button",
            text: "Cancel task",
            on: { click: () => cancelTask(task.id) },
          })
        : null,
    ]);
    const errorParts = [];
    if (task.error_code) errorParts.push(task.error_code);
    if (task.error_message) errorParts.push(task.error_message);
    if (task.unschedulable_reason) errorParts.push(task.unschedulable_reason);
    return node("section", { className: "detail-section" }, [
      actions,
      errorParts.length
        ? node("div", { className: "inline-error", text: errorParts.join("\n") })
        : node("p", { className: "muted", text: "State and timestamps are reported by the control plane." }),
    ]);
  }

  function renderTaskProgress(task) {
    const terminal = TERMINAL_TASK_STATUSES.has(task.status);
    const finalTone = task.status === "succeeded" ? "good" : terminal ? "bad" : "";
    const steps = [
      ["Created", Boolean(task.created_at)],
      ["Queued", Boolean(task.queued_at)],
      ["Assigned", Boolean(task.assigned_at)],
      ["Running", Boolean(task.started_at)],
      [terminal ? task.status.replaceAll("_", " ") : "Final", terminal],
    ];
    return detailSection(
      "Lifecycle",
      node(
        "div",
        { className: "task-track", attrs: { "aria-label": `Current task state: ${task.status}` } },
        steps.map(([label, done], index) =>
          node("div", {
            className: `track-step${done ? " done" : ""}${index === 4 ? ` final ${finalTone}` : ""}`,
            text: label,
          }),
        ),
      ),
    );
  }

  function renderTaskParameters(task) {
    const parameterFields = [
      { label: "Image", value: "image" },
      { label: "Command", value: (item) => item.command.join(" ") },
      { label: "Runtime type", value: "runtime_type" },
      { label: "Workload type", value: "workload_type" },
      { label: "CPU limit", value: (item) => `${item.cpu_limit} cores / ${item.cpu_millicores}m` },
      { label: "Memory limit", value: (item) => `${item.memory_limit_mb} MB` },
      { label: "Timeout", value: (item) => `${item.timeout_seconds}s` },
      { label: "Retry", value: (item) => `${item.retry_count} / ${item.max_retries}` },
      { label: "Network enabled", value: "network_enabled" },
      { label: "Priority", value: "priority" },
      { label: "Preemptible", value: "preemptible" },
      { label: "Accelerator request", value: (item) => item.accelerator_request_json },
      { label: "Selected vendor", value: "selected_vendor" },
      { label: "Selected kind", value: "selected_kind" },
      { label: "Selected model", value: "selected_model" },
      { label: "Runtime profile", value: (item) => [item.runtime_profile_id, item.runtime_profile_version].filter(Boolean).join(" @ ") },
      { label: "Model variant", value: "model_variant_id" },
      { label: "Allocation authority", value: "allocation_authority" },
      { label: "GPU device IDs", value: "gpu_device_ids" },
      { label: "Worker", value: "worker_id" },
      { label: "Execution", value: "execution_id" },
      { label: "Lease expires", value: (item) => formatTime(item.lease_expires_at, true) },
    ];
    const environmentKeys = Object.keys(task.environment || {}).sort();
    const environment = environmentKeys.length
      ? keyValueList(Object.fromEntries(environmentKeys.map((key) => [key, "MASKED"])))
      : node("p", { className: "muted", text: "No environment variables." });
    return node("div", {}, [
      detailSection("Parameters", detailGrid(task, parameterFields)),
      detailSection("Environment", environment),
    ]);
  }

  function renderTaskScheduling(result) {
    if (result.status === "rejected") return detailSection("Scheduling", unavailable(result.reason, true));
    const scheduling = result.value;
    return detailSection(
      "Scheduling",
      node("div", { className: "content-stack" }, [
        detailGrid(scheduling, [
          { label: "State", value: "state" },
          { label: "Reason", value: "reason" },
          { label: "Considered workers", value: "considered_workers" },
          { label: "Attempts", value: "attempts_total" },
          { label: "Latest attempt", value: (item) => formatTime(item.latest_attempt_at, true) },
        ]),
        node("div", { className: "two-column" }, [
          node("div", {}, [node("h3", { text: "Rejection reasons" }), keyValueList(scheduling.rejections)]),
          node("div", {}, [node("h3", { text: "Outcomes" }), keyValueList(scheduling.outcomes)]),
        ]),
      ]),
    );
  }

  function renderTaskTimeline(result) {
    if (result.status === "rejected") return detailSection("Timeline", unavailable(result.reason, true));
    const events = result.value.events || [];
    if (!events.length) {
      return detailSection("Timeline", node("p", { className: "muted", text: "No task events recorded yet." }));
    }
    const timeline = node("div", { className: "timeline" });
    for (const event of events) {
      const transition = event.from_status ? `${event.from_status} → ${event.status}` : event.status;
      timeline.append(
        node("div", { className: "timeline-item" }, [
          node("strong", { text: `${event.event_type} · ${transition}` }),
          node("span", { text: `${formatTime(event.created_at, true)} · sequence ${event.sequence}` }),
          event.details && Object.keys(event.details).length
            ? node("span", { className: "mono", text: formatDetails(event.details) })
            : null,
        ]),
      );
    }
    return detailSection("Timeline", timeline);
  }

  function renderTaskLogs(result) {
    if (result.status === "rejected") return detailSection("Logs", unavailable(result.reason, true));
    const logs = result.value.logs || [];
    const autoScroll = node("input", { type: "checkbox", checked: state.logsAutoScroll });
    autoScroll.addEventListener("change", () => {
      state.logsAutoScroll = autoScroll.checked;
    });
    const toolbar = node("div", { className: "log-toolbar" }, [
      node("span", { className: "muted", text: `${logs.length} lines · stdout/stderr/system` }),
      node("label", { className: "toggle-label" }, [autoScroll, node("span", { text: "Auto scroll" })]),
    ]);
    if (!logs.length) {
      return detailSection("Logs", node("div", {}, [toolbar, node("p", { className: "muted", text: "No logs available yet." })]));
    }
    const viewer = node("div", { className: "log-viewer", id: "task-log-viewer" });
    for (const entry of logs) {
      viewer.append(
        node("div", { className: `log-line ${entry.stream}` }, [
          node("span", { className: "log-time", text: formatTime(entry.timestamp, true) }),
          node("span", { className: "log-stream", text: entry.stream }),
          node("span", { className: "log-content", text: entry.content }),
        ]),
      );
    }
    return detailSection("Logs", node("div", {}, [toolbar, viewer]));
  }

  async function cancelTask(taskId) {
    if (!window.confirm(`Cancel task ${shortId(taskId)}? The backend will apply its normal cancellation semantics.`)) return;
    try {
      await api(`/api/v1/tasks/${encodeURIComponent(taskId)}/cancel`, {
        method: "POST",
        channel: "action:cancel-task",
      });
      await refreshTaskDetail();
      if (state.page === "tasks") await renderTasks();
    } catch (error) {
      showGlobalError(error);
    }
  }

  function parseCommand(value) {
    const args = [];
    let current = "";
    let quote = null;
    let escaped = false;
    let started = false;
    for (const character of value.trim()) {
      if (escaped) {
        current += character;
        escaped = false;
        started = true;
      } else if (character === "\\" && quote !== "'") {
        escaped = true;
        started = true;
      } else if (quote) {
        if (character === quote) quote = null;
        else current += character;
        started = true;
      } else if (character === "'" || character === '"') {
        quote = character;
        started = true;
      } else if (/\s/.test(character)) {
        if (started) {
          args.push(current);
          current = "";
          started = false;
        }
      } else {
        current += character;
        started = true;
      }
    }
    if (escaped || quote) throw new Error("Command contains an unfinished quote or escape.");
    if (started) args.push(current);
    if (!args.length) throw new Error("Command must contain an executable.");
    return args;
  }

  function acceleratorPayload(formData) {
    const count = Number(formData.get("accelerator_count") || 0);
    if (!count) return null;
    const vendor = String(formData.get("accelerator_vendor") || "nvidia");
    const kind = vendor === "huawei-ascend" ? "npu" : "gpu";
    const model = String(formData.get("accelerator_model") || "").trim();
    const profile = String(formData.get("runtime_profile") || "").trim();
    const capabilities = String(formData.get("capabilities") || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const result = {
      count,
      memory_mb_per_device: Number(formData.get("accelerator_memory") || 0),
      allowed_vendors: [vendor],
      allowed_kinds: [kind],
      allowed_models: model ? [model] : [],
      required_capabilities: capabilities,
      selection_policy: vendor === "huawei-ascend" ? "ascend-only" : "nvidia-only",
    };
    if (profile) result.runtime_profile = profile;
    return result;
  }

  function openRunTask() {
    const error = query("#run-task-error");
    error.textContent = "";
    error.classList.add("hidden");
    query("#run-task-dialog").showModal();
  }

  async function submitRunTask(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const errorTarget = query("#run-task-error");
    const submit = query("#run-task-submit");
    errorTarget.classList.add("hidden");
    submit.disabled = true;
    try {
      const payload = {
        image: String(formData.get("image") || "").trim(),
        command: parseCommand(String(formData.get("command") || "")),
        runtime_type: String(formData.get("runtime_type") || "docker"),
        cpu_limit: Number(formData.get("cpu_limit")),
        memory_limit_mb: Number(formData.get("memory_limit_mb")),
        max_retries: Number(formData.get("max_retries") || 0),
        network_enabled: formData.get("network_enabled") === "on",
        priority: Number(formData.get("priority") || 50),
        preemptible: formData.get("preemptible") === "on",
      };
      const timeout = String(formData.get("timeout_seconds") || "").trim();
      if (timeout) payload.timeout_seconds = Number(timeout);
      const accelerator = acceleratorPayload(formData);
      if (accelerator) payload.accelerator = accelerator;
      const created = await api("/api/v1/tasks", {
        method: "POST",
        body: payload,
        headers: { "Idempotency-Key": createIdempotencyKey() },
        channel: "action:create-task",
      });
      query("#run-task-dialog").close();
      goToPage("tasks");
      showTask({ id: created.id });
    } catch (error) {
      const displayedError =
        error instanceof ApiError ? error : new ApiError(0, "INVALID_FORM", error.message);
      errorTarget.textContent = formatApiError(displayedError);
      errorTarget.classList.remove("hidden");
    } finally {
      submit.disabled = false;
    }
  }

  async function renderServices() {
    const page = currentListPage("services");
    const cursorSuffix = listCursorSuffix(page);
    try {
      const payload = await api(
        `/api/v1/services?limit=${LIST_PAGE_SIZE}${cursorSuffix}`,
        { channel: "page:services" },
      );
      const services = payload.items || [];
      state.lastData.services = services;
      if (!services.length) {
        replace("#services-content", [
          node("div", { className: "content-stack" }, [
            emptyState(
              "No services yet",
              "Deploy a model endpoint and inspect its desired, actual, and healthy replicas.",
              "Deploy a service",
              openDeployService,
            ),
            page.history.length
              ? listPagination("services", payload.pagination || {}, 0, renderServices)
              : null,
          ]),
        ]);
        return;
      }
      const columns = [
        { label: "Name", render: (service) => node("strong", { text: service.name }) },
        { label: "Status", render: (service) => badge(service.status) },
        { label: "Model", render: (service) => node("span", { className: "truncate mono", text: service.model, title: service.model }) },
        { label: "Runtime", render: (service) => `${service.runtime} / ${service.runtime_type}` },
        {
          label: "Selection",
          render: (service) => [service.selected_vendor, service.selected_kind, service.selected_model].filter(Boolean).join(" / ") || "CPU / pending",
        },
        { label: "Replicas", render: renderReplicaRatio },
        { label: "Updated", render: (service) => formatTime(service.updated_at) },
      ];
      replace("#services-content", [
        node("div", { className: "content-stack" }, [
          dataTable(columns, services, showService),
          listPagination("services", payload.pagination || {}, services.length, renderServices),
        ]),
      ]);
    } catch (error) {
      if (error.name === "AbortError") return;
      replace("#services-content", [unavailable(error)]);
    }
  }

  function renderReplicaRatio(service) {
    const tone = service.healthy_replicas >= service.desired_replicas ? "good" : service.healthy_replicas > 0 ? "warn" : "bad";
    return node("div", { className: "replica-ratio" }, [
      node("strong", {
        text: `Healthy ${service.healthy_replicas} / Actual ${service.actual_replicas} / Desired ${service.desired_replicas}`,
      }),
      progressBar(service.healthy_replicas, service.desired_replicas, tone),
    ]);
  }

  function showService(service) {
    openDrawer("service", service.id, service.name || `Service ${shortId(service.id)}`);
    refreshServiceDetail().catch(showGlobalError);
  }

  async function refreshServiceDetail() {
    if (!state.detail || state.detail.type !== "service") return;
    const originalId = state.detail.id;
    const id = encodeURIComponent(originalId);
    const results = await Promise.allSettled([
      api(`/api/v1/services/${id}`, { channel: "detail:service" }),
      api(`/api/v1/services/${id}/replicas`, { channel: "detail:service:replicas" }),
    ]);
    if (!state.detail || state.detail.type !== "service" || state.detail.id !== originalId) return;
    const service = settledValue(results[0]);
    if (!service) {
      replace("#drawer-content", [unavailable(results[0].reason)]);
      state.detailTerminal = true;
      return;
    }
    state.detailTerminal = ["stopped", "failed"].includes(service.status);
    query("#drawer-title").textContent = service.name;
    const sections = [
      renderServiceHeader(service),
      detailSection("Replica convergence", renderReplicaRatio(service)),
      renderServiceParameters(service),
      renderServiceAutoscaling(service),
      renderServiceReplicas(results[1]),
    ];
    replace("#drawer-content", sections);
  }

  function renderServiceHeader(service) {
    return node("section", { className: "detail-section" }, [
      node("div", { className: "section-heading" }, [
        node("div", {}, [badge(service.status), node("p", { className: "metric-detail mono", text: service.id })]),
        node("div", {}, [
          node("button", {
            className: "button compact",
            type: "button",
            text: "Scale",
            disabled: ["stopping", "stopped"].includes(service.status),
            on: { click: () => scaleService(service) },
          }),
          " ",
          node("button", {
            className: "button danger compact",
            type: "button",
            text: "Stop",
            disabled: ["stopping", "stopped"].includes(service.status),
            on: { click: () => stopService(service) },
          }),
        ]),
      ]),
      service.error_message || service.scheduling_reason
        ? node("div", {
            className: "inline-error",
            text: [service.scheduling_reason, service.error_message].filter(Boolean).join("\n"),
          })
        : node("p", { className: "muted", text: "Replica counts are reconciled by the existing serving control plane." }),
    ]);
  }

  function renderServiceParameters(service) {
    return detailSection(
      "Service parameters",
      node("div", { className: "content-stack" }, [
        detailGrid(service, [
          { label: "Model", value: "model" },
          { label: "Model revision", value: "model_revision" },
          { label: "Image", value: "image" },
          { label: "Runtime", value: "runtime" },
          { label: "Runtime type", value: "runtime_type" },
          { label: "CPU", value: (item) => `${item.cpu_millicores}m` },
          { label: "Memory", value: (item) => `${item.memory_mb} MB` },
          { label: "Accelerators", value: (item) => `${item.gpu_count} × ${item.gpu_model || "unspecified"}` },
          { label: "Accelerator memory", value: (item) => `${item.gpu_memory_mb} MB / device` },
          { label: "Selected vendor", value: "selected_vendor" },
          { label: "Selected kind", value: "selected_kind" },
          { label: "Selected model", value: "selected_model" },
          { label: "Logical model", value: "logical_model_id" },
          { label: "Model variant", value: "model_variant_id" },
          { label: "Runtime profile", value: (item) => [item.runtime_profile_id, item.runtime_profile_version].filter(Boolean).join(" @ ") },
          { label: "Profile digest", value: "runtime_profile_digest" },
          { label: "Allocation authority", value: "allocation_authority" },
          { label: "Resource name", value: "accelerator_resource_name" },
          { label: "Selection policy", value: "selection_policy" },
          { label: "Eligible nodes", value: "eligible_node_names" },
          { label: "Tensor parallel", value: "tensor_parallel_size" },
          { label: "dtype", value: "dtype" },
          { label: "GPU memory utilization", value: "gpu_memory_utilization" },
          { label: "Max model length", value: "max_model_len" },
          { label: "Generation", value: "generation" },
          { label: "Version", value: "version" },
          { label: "Created", value: (item) => formatTime(item.created_at, true) },
          { label: "Updated", value: (item) => formatTime(item.updated_at, true) },
          { label: "Last scaled", value: (item) => formatTime(item.last_scaled_at, true) },
        ]),
        node("div", {}, [node("h3", { text: "Scheduling details" }), keyValueList(service.scheduling_details)]),
      ]),
    );
  }

  function renderServiceAutoscaling(service) {
    const autoscaling = service.autoscaling || {};
    return detailSection(
      "Autoscaling",
      detailGrid(autoscaling, [
        { label: "Enabled", value: "enabled" },
        { label: "Minimum replicas", value: "min_replicas" },
        { label: "Maximum replicas", value: "max_replicas" },
        { label: "Target concurrency", value: "target_concurrency" },
        { label: "Cooldown seconds", value: "cooldown_seconds" },
      ]),
    );
  }

  function renderServiceReplicas(result) {
    if (result.status === "rejected") return detailSection("Replicas", unavailable(result.reason, true));
    const replicas = result.value.items || [];
    if (!replicas.length) {
      return detailSection("Replicas", node("p", { className: "muted", text: "No replica records yet." }));
    }
    const columns = [
      { label: "Ordinal", key: "ordinal" },
      { label: "Status", render: (replica) => badge(replica.status) },
      { label: "Health", render: (replica) => badge(replica.health) },
      { label: "Worker / node", render: (replica) => `${shortId(replica.worker_id)} / ${valueOrDash(replica.assigned_node_name)}` },
      { label: "Endpoint", render: (replica) => node("span", { className: "truncate mono", text: valueOrDash(replica.endpoint_url), title: replica.endpoint_url || "" }) },
      { label: "Generation", key: "generation" },
      { label: "Updated", render: (replica) => formatTime(replica.updated_at) },
      { label: "Error", render: (replica) => replica.error_code || replica.error_message || "—" },
    ];
    return detailSection("Replicas", dataTable(columns, replicas));
  }

  async function scaleService(service) {
    const raw = window.prompt("Desired replica count (0-1000)", String(service.desired_replicas));
    if (raw === null) return;
    const replicas = Number(raw);
    if (!Number.isInteger(replicas) || replicas < 0 || replicas > 1000) {
      showGlobalError(new ApiError(0, "INVALID_REPLICA_COUNT", "Replica count must be an integer from 0 to 1000."));
      return;
    }
    if (!window.confirm(`Scale ${service.name} from ${service.desired_replicas} to ${replicas} desired replicas?`)) return;
    try {
      await api(`/api/v1/services/${encodeURIComponent(service.id)}/scale`, {
        method: "POST",
        body: { replicas },
        channel: "action:scale-service",
      });
      await refreshServiceDetail();
      if (state.page === "services") await renderServices();
    } catch (error) {
      showGlobalError(error);
    }
  }

  async function stopService(service) {
    if (!window.confirm(`Stop ${service.name}? Desired replicas will be reconciled to zero by the existing API.`)) return;
    try {
      await api(`/api/v1/services/${encodeURIComponent(service.id)}/stop`, {
        method: "POST",
        channel: "action:stop-service",
      });
      await refreshServiceDetail();
      if (state.page === "services") await renderServices();
    } catch (error) {
      showGlobalError(error);
    }
  }

  function syncTensorParallelSize(form) {
    const acceleratorCount = Number(form.elements.accelerator_count.value || 0);
    form.elements.tensor_parallel_size.value = String(Math.max(1, acceleratorCount));
  }

  function syncServingRuntime(form) {
    const runtime = form.elements.runtime.value;
    const runtimeType = form.elements.runtime_type;
    const acceleratorCount = form.elements.accelerator_count;
    const model = form.elements.model;
    const logicalModelId = form.elements.logical_model_id;
    const modelVariantId = form.elements.model_variant_id;
    const acceleratorVendor = form.elements.accelerator_vendor;
    const runtimeProfile = form.elements.runtime_profile;
    if (runtime === "fake") {
      runtimeType.value = "fake";
      runtimeType.disabled = true;
      acceleratorCount.value = "0";
      acceleratorCount.disabled = true;
      acceleratorCount.min = "0";
      model.disabled = false;
      model.required = true;
      logicalModelId.disabled = true;
      logicalModelId.required = false;
      modelVariantId.disabled = true;
      acceleratorVendor.value = "nvidia";
      acceleratorVendor.disabled = true;
      runtimeProfile.disabled = true;
      syncTensorParallelSize(form);
      return;
    }
    runtimeType.disabled = false;
    acceleratorCount.disabled = false;
    if (runtimeType.value === "fake") runtimeType.value = "docker";
    const kubernetesVllm = runtime === "vllm" && runtimeType.value === "kubernetes";
    model.disabled = kubernetesVllm;
    model.required = !kubernetesVllm;
    logicalModelId.disabled = !kubernetesVllm;
    logicalModelId.required = kubernetesVllm;
    modelVariantId.disabled = !kubernetesVllm;
    if (!kubernetesVllm) acceleratorVendor.value = "nvidia";
    acceleratorVendor.disabled = !kubernetesVllm;
    runtimeProfile.disabled = !kubernetesVllm;
    acceleratorCount.min = kubernetesVllm ? "1" : "0";
    if (kubernetesVllm && Number(acceleratorCount.value) < 1) acceleratorCount.value = "1";
    syncTensorParallelSize(form);
  }

  function openDeployService() {
    const error = query("#deploy-service-error");
    error.textContent = "";
    error.classList.add("hidden");
    syncServingRuntime(query("#deploy-service-form"));
    query("#deploy-service-dialog").showModal();
  }

  async function submitDeployService(event) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const errorTarget = query("#deploy-service-error");
    const submit = query("#deploy-service-submit");
    errorTarget.classList.add("hidden");
    submit.disabled = true;
    try {
      const model = String(formData.get("model") || "").trim();
      const logicalModelId = String(formData.get("logical_model_id") || "").trim();
      const runtime = String(formData.get("runtime") || "vllm");
      const runtimeType = String(form.elements.runtime_type.value || "docker");
      if (runtime === "vllm" && runtimeType === "kubernetes" && !logicalModelId) {
        throw new Error("Logical model ID is required for Kubernetes vLLM.");
      }
      if (!model && !logicalModelId) throw new Error("Model or Logical model ID is required.");
      const payload = {
        name: String(formData.get("name") || "").trim(),
        runtime,
        runtime_type: runtimeType,
        replicas: Number(formData.get("replicas")),
        cpu_millicores: Number(formData.get("cpu_millicores")),
        memory_mb: Number(formData.get("memory_mb")),
        tensor_parallel_size: Number(formData.get("tensor_parallel_size")),
        dtype: String(formData.get("dtype") || "auto"),
        gpu_memory_utilization: Number(formData.get("gpu_memory_utilization")),
      };
      if (logicalModelId) payload.logical_model_id = logicalModelId;
      else payload.model = model;
      const variantId = String(formData.get("model_variant_id") || "").trim();
      if (variantId) payload.model_variant_id = variantId;
      const maxModelLength = String(formData.get("max_model_len") || "").trim();
      if (maxModelLength) payload.max_model_len = Number(maxModelLength);
      for (const field of ["image", "model_revision"]) {
        const value = String(formData.get(field) || "").trim();
        if (value) payload[field] = value;
      }
      const accelerator = acceleratorPayload(formData);
      if (accelerator) payload.accelerator = accelerator;
      const created = await api("/api/v1/services", {
        method: "POST",
        body: payload,
        channel: "action:create-service",
      });
      query("#deploy-service-dialog").close();
      goToPage("services");
      showService(created);
    } catch (error) {
      errorTarget.textContent = formatApiError(error instanceof ApiError ? error : new ApiError(0, "INVALID_FORM", error.message));
      errorTarget.classList.remove("hidden");
    } finally {
      submit.disabled = false;
    }
  }

  async function renderWorkers() {
    const page = currentListPage("workers");
    const cursorSuffix = listCursorSuffix(page);
    try {
      const payload = await api(
        `/api/v1/workers?limit=${LIST_PAGE_SIZE}${cursorSuffix}`,
        { channel: "page:workers" },
      );
      const workers = payload.items || [];
      state.lastData.workers = workers;
      if (!workers.length) {
        replace("#workers-content", [
          node("div", { className: "content-stack" }, [
            emptyState(
              "No workers registered",
              "Check worker processes, control-plane readiness, and registration credentials. Capacity appears after a worker heartbeat.",
            ),
            page.history.length
              ? listPagination("workers", payload.pagination || {}, 0, renderWorkers)
              : null,
          ]),
        ]);
        return;
      }
      const columns = [
        {
          label: "Worker",
          render: (worker) =>
            node("div", {}, [
              node("strong", { text: worker.hostname }),
              node("small", { className: "mono", text: worker.node_name || shortId(worker.id), title: worker.id }),
            ]),
        },
        { label: "Status", render: (worker) => badge(worker.status) },
        { label: "Runtimes", render: (worker) => worker.runtime_types.join(", ") },
        { label: "Slots", render: (worker) => capacityCell("slots", worker.running_tasks, worker.concurrency) },
        {
          label: "CPU reserved",
          render: (worker) => capacityCell("cores", Number(worker.reserved_cpu || 0) * 1000, worker.cpu_allocatable_millicores, "m"),
        },
        {
          label: "Memory reserved",
          render: (worker) => capacityCell("memory", worker.reserved_memory_mb, worker.memory_allocatable_mb, " MB"),
        },
        { label: "GPU reserved", render: (worker) => capacityCell("devices", worker.reserved_gpus, worker.gpu_count) },
        { label: "Heartbeat", render: (worker) => formatTime(worker.last_heartbeat_at, true) },
      ];
      replace("#workers-content", [
        node("div", { className: "content-stack" }, [
          dataTable(columns, workers, showWorker),
          listPagination("workers", payload.pagination || {}, workers.length, renderWorkers),
        ]),
      ]);
    } catch (error) {
      if (error.name === "AbortError") return;
      replace("#workers-content", [unavailable(error)]);
    }
  }

  function capacityCell(label, current, total, suffix = "") {
    const tone = Number(total) > 0 && Number(current) > Number(total) ? "bad" : Number(total) > 0 && Number(current) / Number(total) > 0.8 ? "warn" : "good";
    return node("div", { className: "capacity-cell" }, [
      node("div", { className: "capacity-label" }, [
        node("span", { text: label }),
        node("span", { text: `${formatNumber(current)}${suffix} / ${formatNumber(total)}${suffix}` }),
      ]),
      progressBar(current, total, tone),
    ]);
  }

  function showWorker(worker) {
    openDrawer("worker", worker.id, worker.hostname || `Worker ${shortId(worker.id)}`);
    refreshWorkerDetail().catch(showGlobalError);
  }

  async function refreshWorkerDetail() {
    if (!state.detail || state.detail.type !== "worker") return;
    const originalId = state.detail.id;
    try {
      const worker = await api(`/api/v1/workers/${encodeURIComponent(originalId)}`, { channel: "detail:worker" });
      if (!state.detail || state.detail.type !== "worker" || state.detail.id !== originalId) return;
      state.detailTerminal = worker.status === "offline";
      query("#drawer-title").textContent = worker.hostname;
      replace("#drawer-content", [
        node("section", { className: "detail-section" }, [
          node("div", { className: "section-heading" }, [
            node("div", {}, [badge(worker.status), node("p", { className: "metric-detail mono", text: worker.id })]),
          ]),
          worker.overcommitted
            ? node("div", { className: "inline-error", text: "Inventory is overcommitted. Review reservations and allocatable capacity." })
            : node("p", { className: "muted", text: "Inventory and reservation state reported by Mini AI Cloud; not live hardware telemetry." }),
        ]),
        detailSection(
          "Capacity",
          node("div", { className: "content-stack" }, [
            capacityCell("Slots", worker.running_tasks, worker.concurrency),
            capacityCell("CPU millicores", Number(worker.reserved_cpu || 0) * 1000, worker.cpu_allocatable_millicores, "m"),
            capacityCell("Memory", worker.reserved_memory_mb, worker.memory_allocatable_mb, " MB"),
            capacityCell("Accelerators", worker.reserved_gpus, worker.gpu_count),
          ]),
        ),
        detailSection(
          "Worker inventory",
          detailGrid(worker, [
            { label: "Hostname", value: "hostname" },
            { label: "Node name", value: "node_name" },
            { label: "Runtime types", value: "runtime_types" },
            { label: "Started", value: (item) => formatTime(item.started_at, true) },
            { label: "Last heartbeat", value: (item) => formatTime(item.last_heartbeat_at, true) },
            { label: "Session", value: "worker_session_id" },
            { label: "CPU cores", value: "cpu_count" },
            { label: "CPU total", value: (item) => `${item.cpu_total_millicores}m` },
            { label: "CPU allocatable", value: (item) => `${item.cpu_allocatable_millicores}m` },
            { label: "Memory total", value: (item) => `${item.memory_total_mb} MB` },
            { label: "Memory allocatable", value: (item) => `${item.memory_allocatable_mb} MB` },
            { label: "GPU count", value: "gpu_count" },
            { label: "GPU model", value: "gpu_model" },
            { label: "GPU memory", value: (item) => `${item.gpu_memory_mb} MB / device` },
            { label: "Docker version", value: "docker_version" },
            { label: "Inventory generation", value: "inventory_generation" },
            { label: "Inventory updated", value: (item) => formatTime(item.inventory_updated_at, true) },
            { label: "Drain reason", value: "drain_reason" },
          ]),
        ),
        detailSection(
          "Placement metadata",
          node("div", { className: "two-column" }, [
            node("div", {}, [node("h3", { text: "Labels" }), keyValueList(worker.labels)]),
            node("div", {}, [node("h3", { text: "Taints" }), keyValueList(Object.fromEntries((worker.taints || []).map((taint, index) => [`taint_${index + 1}`, taint])))]),
          ]),
        ),
      ]);
    } catch (error) {
      if (error.name === "AbortError") return;
      replace("#drawer-content", [unavailable(error)]);
      state.detailTerminal = true;
    }
  }

  async function renderUsage() {
    if (!state.project) {
      replace("#usage-content", [emptyState("Project unavailable", "Reconnect with a project-scoped API key.")]);
      return;
    }
    const hours = Number(query("#usage-window").value || 24);
    const to = new Date();
    const from = new Date(to.getTime() - hours * 60 * 60 * 1000);
    const root = `/api/v1/projects/${encodeURIComponent(state.project.id)}`;
    const windowQuery = `from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(to.toISOString())}`;
    const [quotaResult, usageResult, costResult] = await Promise.allSettled([
      api(`${root}/quota`, { channel: "page:usage:quota" }),
      api(`${root}/usage?${windowQuery}`, { channel: "page:usage:usage" }),
      api(`${root}/cost?${windowQuery}`, { channel: "page:usage:cost" }),
    ]);
    const content = [];
    const usage = settledValue(usageResult);
    if (usage) content.push(renderUsageSummary(usage, hours));
    else content.push(detailSection(`Usage · last ${usageWindowLabel(hours)}`, unavailable(usageResult.reason, true)));
    const quota = settledValue(quotaResult);
    content.push(quota ? renderQuota(quota) : detailSection("Project quota", unavailable(quotaResult.reason, true)));
    const cost = settledValue(costResult);
    content.push(cost ? renderCost(cost, hours) : detailSection("Cost", unavailable(costResult.reason, true)));
    if (usage && usage.gpu_breakdown && usage.gpu_breakdown.length) {
      content.push(
        detailSection(
          "Accelerator usage breakdown",
          dataTable(
            [
              { label: "Model", render: (item) => item.gpu_model || "Unspecified" },
              { label: "Accelerator seconds", render: (item) => formatNumber(item.gpu_seconds) },
            ],
            usage.gpu_breakdown,
          ),
        ),
      );
    }
    replace("#usage-content", content);
  }

  function usageWindowLabel(hours) {
    if (hours === 1) return "1 hour";
    if (hours === 24) return "24 hours";
    if (hours === 168) return "7 days";
    if (hours === 720) return "30 days";
    return `${hours} hours`;
  }

  function renderUsageSummary(usage, hours) {
    const serving = usage.serving || {};
    return node("section", {}, [
      node("div", { className: "section-heading" }, [
        node("div", {}, [node("p", { className: "eyebrow", text: `Last ${usageWindowLabel(hours)}` }), node("h2", { text: "Settled usage" })]),
        node("span", { className: "muted", text: `Basis: ${usage.settlement_basis}` }),
      ]),
      node("div", { className: "card-grid summary-grid" }, [
        metricCard("Executions", formatNumber(usage.execution_count, 0), "Completed task executions in window"),
        metricCard("CPU seconds", formatNumber(usage.cpu_seconds), "Settled CPU allocation time"),
        metricCard("Memory GB-seconds", formatNumber(usage.memory_gb_seconds), "Settled memory allocation time"),
        metricCard("Accelerator seconds", formatNumber(usage.gpu_seconds), "Settled task accelerator time"),
        metricCard("Serving requests", formatNumber(serving.request_count, 0), `${formatNumber(serving.reported_total_tokens, 0)} reported tokens`),
        metricCard("Serving accelerator", formatNumber(serving.replica_gpu_seconds), `${formatNumber(serving.allocated_gpu_seconds)} allocated GPU seconds`),
      ]),
      detailSection(
        "Serving usage details",
        detailGrid(serving, [
          { label: "Request count", value: "request_count" },
          { label: "Requests with reported tokens", value: "requests_with_reported_token_usage" },
          { label: "Input tokens", value: "reported_input_tokens" },
          { label: "Output tokens", value: "reported_output_tokens" },
          { label: "Total tokens", value: "reported_total_tokens" },
          { label: "Allocated accelerator seconds", value: "allocated_gpu_seconds" },
          { label: "Replica accelerator seconds", value: "replica_gpu_seconds" },
        ]),
      ),
    ]);
  }

  function renderQuota(quota) {
    const limits = quota.limits || {};
    const current = quota.state || {};
    const quotaDefinitions = [
      ["Queued tasks", "max_queued_tasks", Number(current.queued_tasks || 0)],
      ["Running tasks", "max_running_tasks", Number(current.running_tasks || 0)],
      ["CPU millicores", "max_cpu_millicores", Number(current.reserved_cpu_millicores || 0) + Number(current.service_reserved_cpu_millicores || 0)],
      ["Memory MB", "max_memory_mb", Number(current.reserved_memory_mb || 0) + Number(current.service_reserved_memory_mb || 0)],
      ["Accelerators", "max_gpus", Number(current.reserved_gpus || 0) + Number(current.service_reserved_gpus || 0)],
      ["NVIDIA GPUs", "max_nvidia_gpus", Number(current.reserved_nvidia_gpus || 0) + Number(current.service_reserved_nvidia_gpus || 0)],
      ["Ascend NPUs", "max_ascend_npus", Number(current.reserved_ascend_npus || 0) + Number(current.service_reserved_ascend_npus || 0)],
      ["Services", "max_services", Number(current.service_count || 0)],
      ["Service replicas", "max_service_replicas", Number(current.service_replicas || 0)],
      ["Artifact bytes", "max_artifact_bytes", Number(current.artifact_bytes || 0)],
      ["Daily cost", "daily_cost_limit", Number(current.daily_reserved_cost || 0) + Number(current.daily_settled_cost || 0)],
    ];
    const grid = node("div", { className: "quota-grid" });
    for (const [label, key, used] of quotaDefinitions) {
      const limit = limits[key];
      const displayLimit = limit === null || limit === undefined ? "Unlimited" : formatNumber(limit);
      const tone = limit !== null && limit !== undefined && Number(limit) > 0 && used / Number(limit) > 0.9 ? "warn" : "good";
      grid.append(
        node("div", { className: "quota-item" }, [
          node("div", {}, [node("span", { text: label }), node("strong", { className: "mono", text: `${formatNumber(used)} / ${displayLimit}` })]),
          progressBar(used, limit, tone),
        ]),
      );
    }
    return detailSection("Project quota", grid);
  }

  function renderCost(cost, hours) {
    const costs = cost.costs || [];
    return detailSection(
      `Cost · last ${usageWindowLabel(hours)}`,
      costs.length
        ? dataTable(
            [
              { label: "Currency", key: "currency" },
              { label: "Cost", render: (item) => `${item.currency} ${formatNumber(item.cost, 4)}` },
              { label: "Executions", render: () => formatNumber(cost.execution_count, 0) },
            ],
            costs,
          )
        : node("p", { className: "muted", text: "No settled cost was returned for this window." }),
    );
  }

  async function renderSystem() {
    const [liveResult, readyResult, healthResult, openapiResult] = await Promise.allSettled([
      api("/livez", { auth: false, channel: "page:system:live", acceptStatuses: [503] }),
      api("/readyz", { auth: false, channel: "page:system:ready", acceptStatuses: [503] }),
      api("/health", { auth: false, channel: "page:system:health", acceptStatuses: [503] }),
      api("/openapi.json", { auth: false, channel: "page:system:openapi" }),
    ]);
    const live = settledValue(liveResult);
    const ready = settledValue(readyResult);
    const health = settledValue(healthResult);
    const openapi = settledValue(openapiResult);
    const healthCards = [
      healthMetric("Liveness", live && live.status, liveResult),
      healthMetric("Readiness", ready && ready.status, readyResult),
      healthMetric("PostgreSQL", health && health.checks && health.checks.postgresql, healthResult),
      healthMetric("Redis", health && health.checks && health.checks.redis, healthResult),
    ];
    const links = node("div", { className: "resource-list" }, [
      systemLink("OpenAPI documentation", "/docs", "Interactive API reference"),
      systemLink("OpenAPI JSON", "/openapi.json", "Machine-readable API contract"),
      systemLink("Prometheus metrics", "/metrics", "Raw control-plane metrics; use Grafana for observability"),
    ]);
    replace("#system-content", [
      node("div", { className: "card-grid health-grid" }, healthCards),
      detailSection(
        "Application",
        detailGrid(
          {
            name: openapi && openapi.info ? openapi.info.title : "Mini AI Cloud",
            version: openapi && openapi.info ? openapi.info.version : null,
            api_base: state.apiBase,
            project: state.project ? `${state.project.name} (${state.project.slug})` : null,
          },
          [
            { label: "Application", value: "name" },
            { label: "Version", value: "version" },
            { label: "API base", value: "api_base" },
            { label: "Project", value: "project" },
          ],
        ),
      ),
      detailSection("Operator links", links),
      node("article", { className: "notice", text: "Workbench presents control-plane state and bounded operations. Prometheus and Grafana remain the source for professional telemetry and time-series analysis." }),
    ]);
  }

  function systemLink(label, path, description) {
    const href = new URL(path, `${state.apiBase}/`).href;
    return node("a", { className: "resource-row", href, target: "_blank", rel: "noreferrer" }, [
      node("div", {}, [node("strong", { text: label }), node("small", { text: description })]),
      node("span", { text: "Open" }),
    ]);
  }

  async function refreshDetail() {
    if (!state.detail) return;
    if (state.detail.type === "task") await refreshTaskDetail();
    if (state.detail.type === "service") await refreshServiceDetail();
    if (state.detail.type === "worker") await refreshWorkerDetail();
  }

  function bindEvents() {
    query("#connection-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      const errorTarget = query("#connection-error");
      const apiKeyInput = query("#api-key");
      const apiKey = apiKeyInput.value;
      apiKeyInput.value = "";
      errorTarget.classList.add("hidden");
      try {
        await connect(query("#api-base").value, apiKey);
      } catch (error) {
        state.apiKey = "";
        errorTarget.textContent = formatApiError(error instanceof ApiError ? error : new ApiError(0, "INVALID_CONNECTION", error.message));
        errorTarget.classList.remove("hidden");
      }
    });
    query("#disconnect-button").addEventListener("click", disconnect);
    for (const navItem of document.querySelectorAll(".nav-item")) {
      navItem.addEventListener("click", () => goToPage(navItem.dataset.page));
    }
    for (const button of document.querySelectorAll('[data-action="open-run-task"]')) {
      button.addEventListener("click", openRunTask);
    }
    for (const button of document.querySelectorAll('[data-action="open-deploy-service"]')) {
      button.addEventListener("click", openDeployService);
    }
    query("#drawer-close").addEventListener("click", closeDrawer);
    query("#drawer-backdrop").addEventListener("click", closeDrawer);
    query("#refresh-button").addEventListener("click", () => refreshCurrentPage().catch(showGlobalError));
    query("#auto-refresh").addEventListener("change", (event) => {
      state.autoRefresh = event.target.checked;
      scheduleRefresh(0);
    });
    query("#task-status-filter").addEventListener("change", () => renderTasks().catch(showGlobalError));
    query("#usage-window").addEventListener("change", () => renderUsage().catch(showGlobalError));
    query("#run-task-form").addEventListener("submit", submitRunTask);
    query("#deploy-service-form").addEventListener("submit", submitDeployService);
    for (const button of document.querySelectorAll('[data-dialog-close]')) {
      button.addEventListener("click", () => button.closest("dialog").close());
    }
    const deployServiceForm = query("#deploy-service-form");
    deployServiceForm.elements.runtime.addEventListener("change", () => {
      syncServingRuntime(deployServiceForm);
    });
    deployServiceForm.elements.runtime_type.addEventListener("change", () => {
      syncServingRuntime(deployServiceForm);
    });
    deployServiceForm.elements.accelerator_count.addEventListener("input", () => {
      syncTensorParallelSize(deployServiceForm);
    });
    syncServingRuntime(deployServiceForm);
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && state.detail) closeDrawer();
    });
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
        state.refreshTimer = null;
        abortAllRequests();
      } else if (state.apiKey) {
        refreshCurrentPage().catch(showGlobalError);
      }
    });
  }

  async function initialize() {
    bindEvents();
    const savedKey = sessionGet(SESSION_API_KEY);
    query("#api-base").value = window.location.origin;
    if (!savedKey) return;
    try {
      await connect(window.location.origin, savedKey);
    } catch (error) {
      state.apiKey = "";
      sessionRemove(SESSION_API_KEY);
      const target = query("#connection-error");
      target.textContent = `Saved session could not reconnect.\n${formatApiError(error)}`;
      target.classList.remove("hidden");
    }
  }

  initialize().catch((error) => {
    const target = query("#connection-error");
    target.textContent = formatApiError(error);
    target.classList.remove("hidden");
  });
})();
