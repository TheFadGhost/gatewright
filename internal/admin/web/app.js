(() => {
"use strict";

const $id = (id) => document.getElementById(id);

const THEMES = ["dark", "light", "contrast"];
const THEME_NAMES = { dark: "dark", light: "light", contrast: "high-contrast" };
const THEME_KEY = "gatewright.admin.theme";
const HISTORY_LIMIT = 60;

const reduceMotion = window.matchMedia
  && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const ui = {
  version: $id("hdr-version"),
  uptime: $id("hdr-uptime"),
  reload: $id("btn-reload"),
  reloadStatus: $id("reload-status"),
  themeBtn: $id("btn-theme"),
  helpBtn: $id("btn-help"),
  srStatus: $id("sr-status"),
  filter: $id("filter-input"),
  routesBody: $id("routes-body"),
  upstreamsBody: $id("upstreams-body"),
  limitersBody: $id("limiters-body"),
  routesSection: $id("routes-section"),
  upstreamsSection: $id("upstreams-section"),
  limitersSection: $id("limiters-section"),
  canvas: $id("latency-canvas"),
  chartEmpty: $id("chart-empty"),
  chartTitle: $id("chart-title"),
  detail: $id("detail"),
  detailTitle: $id("detail-title"),
  detailBody: $id("detail-body"),
  detailClose: $id("detail-close"),
  overlay: $id("overlay"),
  overlayClose: $id("overlay-close"),
  errorPanel: $id("error-panel"),
  errorMessage: $id("error-message"),
  btnRetry: $id("btn-retry"),
};

const state = {
  data: null,
  loaded: false,
  filterText: "",
  selected: null,
  history: new Map(),
  source: null,
  lastFocus: null,
};

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined && text !== null) n.textContent = String(text);
  return n;
}

function announce(msg) {
  ui.srStatus.textContent = msg;
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function fmtNum(v, digits) {
  if (!Number.isFinite(v)) return "-";
  return v.toFixed(digits);
}

function fmtMs(ms) {
  return fmtNum(ms, ms >= 100 ? 0 : 1) + " ms";
}

function fmtUptime(sec) {
  sec = Math.floor(sec);
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  const parts = [];
  if (d) parts.push(d + "d");
  if (d || h) parts.push(h + "h");
  if (d || h || m) parts.push(m + "m");
  parts.push(s + "s");
  return parts.join(" ");
}

function fmtClock(t) {
  const p = (n) => String(n).padStart(2, "0");
  return p(t.getHours()) + ":" + p(t.getMinutes()) + ":" + p(t.getSeconds());
}

function secsLeft(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return Math.round((t - Date.now()) / 1000);
}

const STATE_CLASS = {
  healthy: "ok",
  probing: "warn",
  ejected: "warn",
  down: "down",
  draining: "draining",
};

function healthCell(stateWord) {
  const wrap = el("span");
  const dot = el("span", "dot");
  dot.classList.add(STATE_CLASS[stateWord] || "draining");
  const word = el("span", "health-word", stateWord || "unknown");
  wrap.appendChild(dot);
  wrap.appendChild(word);
  return wrap;
}

// Reconciles tbody rows against keyed items so live refreshes update cells in
// place instead of rebuilding the table.
function reconcile(tbody, items, keyFn, buildRow, updateRow) {
  const byKey = new Map();
  for (const row of Array.from(tbody.children)) {
    if (row.dataset.key) byKey.set(row.dataset.key, row);
  }
  const seen = new Set();
  items.forEach((item) => {
    const key = keyFn(item);
    seen.add(key);
    let row = byKey.get(key);
    if (!row) {
      row = buildRow(item);
      row.dataset.key = key;
      tbody.appendChild(row);
    }
    updateRow(row, item);
  });
  for (const [key, row] of byKey) {
    if (!seen.has(key)) row.remove();
  }
  items.forEach((item) => {
    const row = tbody.querySelector('tr[data-key="' + CSS.escape(keyFn(item)) + '"]');
    if (row) tbody.appendChild(row);
  });
}

function emptyRow(cols, sentence) {
  const tr = el("tr", "empty-row");
  const td = el("td");
  td.colSpan = cols;
  td.appendChild(el("span", "empty", sentence));
  tr.appendChild(td);
  return tr;
}

function skeletons(tbody, cols, count) {
  for (let i = 0; i < count; i++) {
    const tr = el("tr", "skel");
    for (let c = 0; c < cols; c++) tr.appendChild(el("td"));
    tbody.appendChild(tr);
  }
}

function clearSkeletons() {
  for (const section of [ui.routesSection, ui.upstreamsSection, ui.limitersSection]) {
    section.setAttribute("aria-busy", "false");
  }
  ui.routesBody.textContent = "";
  ui.upstreamsBody.textContent = "";
  ui.limitersBody.textContent = "";
}

// ---- Routes ----

function routeMatches(r) {
  const q = state.filterText;
  if (!q) return true;
  return (r.name + " " + r.match + " " + r.upstream + " " + (r.limiter_name || ""))
    .toLowerCase().includes(q);
}

function visibleRoutes() {
  if (!state.data) return [];
  return state.data.routes.filter(routeMatches);
}

function limiterFor(r) {
  if (!state.data) return null;
  return state.data.limiters.find(
    (l) => l.route === r.name && l.name === r.limiter_name) || null;
}

function poolHealth(poolName) {
  const order = { down: 4, ejected: 3, probing: 2 };
  let worst = "healthy";
  for (const p of state.data.pools) {
    if (p.name !== poolName) continue;
    for (const t of p.targets) {
      if ((order[t.state] || 0) > (order[worst] || 0)) worst = t.state;
      if (worst === "down") return worst;
    }
  }
  return worst;
}

function usageLabel(frac) {
  if (typeof frac !== "number" || frac < 0) return "-";
  if (frac > 1) return ">100%";
  return Math.round(frac * 100) + "%";
}

function usageBar(frac, cls) {
  const bar = el("div", "bar");
  if (typeof frac === "number" && frac > 0) {
    const seg = el("span", "seg " + cls);
    seg.style.width = Math.min(frac, 1) * 100 + "%";
    bar.appendChild(seg);
  }
  return bar;
}

function renderRoutes() {
  const routes = visibleRoutes();
  if (state.data.routes.length === 0) {
    ui.routesBody.textContent = "";
    ui.routesBody.appendChild(emptyRow(7, "No routes configured."));
    return;
  }
  reconcile(ui.routesBody, routes,
    (r) => r.name,
    (r) => {
      const tr = el("tr", "row");
      tr.tabIndex = -1;
      tr.setAttribute("aria-selected", "false");
      const nameTd = el("td", "name");
      const matchTd = el("td");
      const upTd = el("td");
      const rpsTd = el("td", "num-cell col-rps");
      const p95Td = el("td", "num-cell col-p95");
      const stTd = el("td", "num-cell col-status");
      const limTd = el("td");
      limTd.style.whiteSpace = "normal";
      tr.append(nameTd, matchTd, upTd, rpsTd, p95Td, stTd, limTd);
      tr.addEventListener("click", () => selectRoute(r.name, true));
      return tr;
    },
    (tr, r) => {
      tr.hidden = !routeMatches(r);
      tr.cells[0].textContent = r.name;
      tr.cells[1].textContent = r.match;
      tr.cells[2].textContent = "";
      tr.cells[2].appendChild(healthCell(poolHealth(r.upstream)));
      tr.cells[2].appendChild(document.createTextNode(" " + r.upstream));
      tr.cells[3].textContent = fmtNum(r.rps, 1);
      tr.cells[4].textContent = r.percentiles && r.percentiles.ok && r.percentiles.p95_ms != null
        ? fmtMs(r.percentiles.p95_ms)
        : "-";
      tr.cells[5].textContent =
        `${r.status_2xx ?? "-"}/${r.status_4xx ?? "-"}/${r.status_5xx ?? "-"}`;
      tr.cells[6].textContent = "";
      if (r.has_limiter) {
        tr.cells[6].appendChild(el("span", null, r.limiter_name || "(limiter)"));
        const lv = limiterFor(r);
        const frac = lv ? lv.usage_fraction : -1;
        if (typeof frac === "number" && frac >= 0) {
          tr.cells[6].appendChild(document.createTextNode(" "));
          tr.cells[6].appendChild(usageBar(frac, "seg-ok"));
        }
        tr.cells[6].appendChild(document.createTextNode(" "));
        tr.cells[6].appendChild(el("span", "dim num", usageLabel(frac)));
      } else {
        tr.cells[6].appendChild(el("span", "dim", "-"));
      }
      tr.classList.toggle("selected", state.selected === r.name);
      tr.setAttribute("aria-selected", state.selected === r.name ? "true" : "false");
    });
}

// ---- Upstreams ----

function renderUpstreams() {
  if (state.data.pools.length === 0) {
    ui.upstreamsBody.textContent = "";
    ui.upstreamsBody.appendChild(emptyRow(6, "No upstreams configured."));
    return;
  }
  const targets = [];
  for (const p of state.data.pools) {
    for (const t of p.targets) targets.push({ pool: p.name, t });
  }
  reconcile(ui.upstreamsBody, targets,
    (x) => x.t.name,
    () => {
      const tr = el("tr");
      for (let i = 0; i < 6; i++) {
        const td = el("td");
        if (i >= 1 && i <= 3) td.className = "num-cell";
        tr.appendChild(td);
      }
      return tr;
    },
    (tr, x) => {
      const t = x.t;
      tr.cells[0].textContent = "";
      tr.cells[0].appendChild(el("span", "name", t.name));
      tr.cells[0].appendChild(document.createTextNode(" "));
      const hc = healthCell(t.state);
      hc.className = "";
      tr.cells[0].appendChild(hc);
      if (t.ejected_until) {
        const left = secsLeft(t.ejected_until);
        if (left != null && left > 0) {
          tr.cells[0].appendChild(document.createTextNode(" (ejected " + left + "s left)"));
        }
      }
      tr.cells[1].textContent = t.circuit;
      tr.cells[1].className = "num-cell";
      tr.cells[2].textContent = String(t.inflight);
      tr.cells[3].textContent = String(t.total_req);
      tr.cells[4].textContent = String(t.total_fail);
      tr.cells[5].textContent = t.ejected_until || "-";
    });
}

// ---- Limiters ----

function renderLimiters() {
  if (state.data.limiters.length === 0) {
    ui.limitersBody.textContent = "";
    ui.limitersBody.appendChild(emptyRow(9, "No limiters configured."));
    return;
  }
  reconcile(ui.limitersBody, state.data.limiters,
    (l) => l.route + "/" + l.name,
    () => {
      const tr = el("tr");
      for (let i = 0; i < 9; i++) {
        const td = el("td");
        if (i === 4 || i === 5 || i === 7 || i === 8) td.className = "num-cell";
        tr.appendChild(td);
      }
      return tr;
    },
    (tr, l) => {
      tr.cells[0].textContent = l.route;
      tr.cells[1].textContent = l.name;
      tr.cells[2].textContent = "";
      tr.cells[2].appendChild(el("span", "tag", l.strategy));
      tr.cells[3].textContent = l.key_type;
      tr.cells[4].textContent = fmtNum(l.allowed_per_sec, 1);
      tr.cells[5].textContent = fmtNum(l.limited_per_sec, 1);
      tr.cells[6].textContent = "";
      const total = (l.allowed_per_sec || 0) + (l.limited_per_sec || 0);
      if (total > 0) {
        const bar = el("div", "bar");
        bar.setAttribute("role", "img");
        bar.setAttribute("aria-label",
          fmtNum(l.allowed_per_sec, 1) + " allowed per second, "
          + fmtNum(l.limited_per_sec, 1) + " limited per second");
        const okSeg = el("span", "seg seg-ok");
        okSeg.style.width = ((l.allowed_per_sec || 0) / total) * 100 + "%";
        const downSeg = el("span", "seg seg-down");
        downSeg.style.left = ((l.allowed_per_sec || 0) / total) * 100 + "%";
        downSeg.style.width = ((l.limited_per_sec || 0) / total) * 100 + "%";
        bar.append(okSeg, downSeg);
        tr.cells[6].appendChild(bar);
      } else {
        tr.cells[6].textContent = "-";
      }
      tr.cells[7].textContent = usageLabel(l.usage_fraction);
      tr.cells[8].textContent = l.evictions > 0 ? String(l.evictions) : "";
    });
}

// ---- Latency chart ----

function pushHistory(data) {
  const ts = Date.parse(data.generated_at);
  const t = Number.isFinite(ts) ? ts : Date.now();
  const alive = new Set();
  for (const r of data.routes) {
    alive.add(r.name);
    let arr = state.history.get(r.name);
    if (!arr) {
      arr = [];
      state.history.set(r.name, arr);
    }
    const pct = r.percentiles;
    arr.push({
      t,
      ok: !!(pct && pct.ok && pct.p50_ms != null && pct.p95_ms != null && pct.p99_ms != null),
      p50: pct ? pct.p50_ms : null,
      p95: pct ? pct.p95_ms : null,
      p99: pct ? pct.p99_ms : null,
    });
    while (arr.length > HISTORY_LIMIT) arr.shift();
  }
  for (const name of Array.from(state.history.keys())) {
    if (!alive.has(name)) state.history.delete(name);
  }
}

function niceScale(maxVal) {
  if (!(maxVal > 0)) maxVal = 1;
  const rough = maxVal / 4;
  const pow = Math.pow(10, Math.floor(Math.log10(rough)));
  const norm = rough / pow;
  let step;
  if (norm <= 1) step = 1;
  else if (norm <= 2) step = 2;
  else if (norm <= 5) step = 5;
  else step = 10;
  step *= pow;
  let top = step * 4;
  if (top < maxVal) top = step * 5;
  return { top, step };
}

const SERIES = [
  { key: "p50", colorVar: "--accent", dash: [] },
  { key: "p95", colorVar: "--warn", dash: [6, 4] },
  { key: "p99", colorVar: "--down", dash: [2, 3] },
];

function drawChart() {
  const canvas = ui.canvas;
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth || canvas.parentElement.clientWidth;
  const cssH = 220;
  canvas.width = Math.max(1, Math.round(cssW * dpr));
  canvas.height = Math.round(cssH * dpr);

  const name = state.selected || firstHistoryName();
  const series = name ? state.history.get(name) : null;

  ui.chartTitle.textContent = name ? "LATENCY - " + name : "LATENCY";

  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  if (!series || series.length === 0) {
    canvas.hidden = true;
    ui.chartEmpty.hidden = false;
    ui.chartEmpty.textContent = name
      ? "No latency data yet for " + name + "."
      : "Select a route to view latency.";
    return;
  }
  canvas.hidden = false;
  ui.chartEmpty.hidden = true;

  const padL = 56, padR = 12, padT = 12, padB = 24;
  const plotW = cssW - padL - padR;
  const plotH = cssH - padT - padB;

  let maxVal = 0;
  for (const pt of series) {
    if (!pt.ok) continue;
    maxVal = Math.max(maxVal, pt.p50, pt.p95, pt.p99);
  }
  const scale = niceScale(maxVal);
  const yTicks = [];
  for (let v = 0; v <= scale.top + scale.step / 2; v += scale.step) yTicks.push(v);

  const axisColor = cssVar("--border") || "#30363d";
  const labelColor = cssVar("--text-dim") || "#9198a1";
  ctx.font = "11px ui-monospace, Consolas, monospace";
  ctx.textBaseline = "middle";

  for (const v of yTicks) {
    const y = padT + plotH - (v / scale.top) * plotH;
    ctx.strokeStyle = axisColor;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillStyle = labelColor;
    ctx.textAlign = "right";
    ctx.fillText(fmtNum(v, 0) + " ms", padL - 6, y);
  }

  const n = series.length;
  const span = Math.max(n - 1, 1);
  const xOf = (i) => padL + (i / span) * plotW;
  const yOf = (v) => padT + plotH - (Math.min(v, scale.top) / scale.top) * plotH;

  const labelIdx = [0, Math.floor((n - 1) / 2), n - 1];
  ctx.fillStyle = labelColor;
  ctx.textAlign = "center";
  for (const i of new Set(labelIdx)) {
    const d = new Date(series[i].t);
    const x = xOf(i);
    ctx.fillText(fmtClock(d), Math.min(Math.max(x, padL), padL + plotW), cssH - 10);
  }

  for (const s of SERIES) {
    ctx.strokeStyle = cssVar(s.colorVar) || "#58a6ff";
    ctx.lineWidth = 1.5;
    ctx.setLineDash(s.dash);
    ctx.beginPath();
    let pen = false;
    for (let i = 0; i < n; i++) {
      const pt = series[i];
      if (!pt.ok || !Number.isFinite(pt[s.key])) {
        pen = false;
        continue;
      }
      const x = xOf(i);
      const y = yOf(pt[s.key]);
      if (pen) ctx.lineTo(x, y);
      else ctx.moveTo(x, y);
      pen = true;
    }
    ctx.stroke();
  }
  ctx.setLineDash([]);
}

function firstHistoryName() {
  if (!state.data || state.data.routes.length === 0) return null;
  return state.data.routes[0].name;
}

// ---- Selection and detail panel ----

function selectRoute(name, viaPointer) {
  if (state.selected === name) {
    if (!viaPointer) openDetail();
    return;
  }
  state.selected = name;
  for (const tr of ui.routesBody.querySelectorAll("tr.row")) {
    const on = tr.dataset.key === name;
    tr.classList.toggle("selected", on);
    tr.setAttribute("aria-selected", on ? "true" : "false");
  }
  drawChart();
  announce("Selected route " + name);
}

function moveSelection(delta) {
  const routes = visibleRoutes();
  if (routes.length === 0) return;
  let idx = routes.findIndex((r) => r.name === state.selected);
  idx = idx === -1 ? (delta > 0 ? 0 : routes.length - 1) : idx + delta;
  idx = Math.min(Math.max(idx, 0), routes.length - 1);
  selectRoute(routes[idx].name, false);
  const row = ui.routesBody.querySelector(
    'tr[data-key="' + CSS.escape(state.selected) + '"]');
  if (row) row.scrollIntoView({ block: "nearest" });
}

function detailPair(dt, ddText) {
  dt.textContent = ddText[0];
  const dd = el("dd");
  if (typeof ddText[1] === "string" || typeof ddText[1] === "number") {
    dd.textContent = String(ddText[1]);
  } else {
    dd.appendChild(ddText[1]);
  }
  return [dt, dd];
}

function openDetail() {
  const name = state.selected;
  const r = state.data && state.data.routes.find((x) => x.name === name);
  if (!r) return;
  ui.detailTitle.textContent = "Route " + r.name;
  ui.detailBody.textContent = "";

  const addPair = (k, v) => {
    const [dt, dd] = detailPair(el("dt"), [k, v]);
    ui.detailBody.append(dt, dd);
  };
  const pctText = (key) =>
    r.percentiles && r.percentiles.ok && r.percentiles[key] != null
      ? fmtMs(r.percentiles[key])
      : "-";
  addPair("match", r.match);
  addPair("requests/sec", fmtNum(r.rps, 1));
  addPair("p50 latency", pctText("p50_ms"));
  addPair("p95 latency", pctText("p95_ms"));
  addPair("p99 latency", pctText("p99_ms"));
  addPair("limiter", r.has_limiter ? r.limiter_name : "none");

  const pool = state.data.pools.find((p) => p.name === r.upstream);
  if (pool) {
    const dt = el("dt");
    dt.textContent = "upstream targets (" + pool.name + ")";
    const list = el("ul", "target-line");
    for (const t of pool.targets) {
      const li = el("li");
      li.appendChild(healthCell(t.state));
      li.appendChild(document.createTextNode(" " + t.name
        + " - inflight " + t.inflight
        + ", circuit " + t.circuit));
      list.appendChild(li);
    }
    const dd = el("dd");
    dd.appendChild(list);
    ui.detailBody.append(dt, dd);
  }

  if (!reduceMotion) ui.detail.style.opacity = "0";
  ui.detail.hidden = false;
  if (!reduceMotion) {
    requestAnimationFrame(() => {
      ui.detail.style.transition = "opacity 120ms ease";
      ui.detail.style.opacity = "1";
      setTimeout(() => { ui.detail.style.transition = ""; }, 140);
    });
  }
  state.lastFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  ui.detail.focus();
  announce("Opened detail for route " + r.name);
}

function closeDetail() {
  if (ui.detail.hidden) return false;
  ui.detail.hidden = true;
  if (state.lastFocus && document.contains(state.lastFocus)) state.lastFocus.focus();
  announce("Closed detail");
  return true;
}

function closeOverlay() {
  if (ui.overlay.hidden) return false;
  ui.overlay.hidden = true;
  announce("Closed shortcuts");
  return true;
}

// ---- Header actions ----

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  ui.themeBtn.textContent = "theme: " + THEME_NAMES[theme];
  try { localStorage.setItem(THEME_KEY, theme); } catch (e) { /* storage blocked */ }
  drawChart();
}

function cycleTheme() {
  const current = document.documentElement.dataset.theme || "dark";
  const next = THEMES[(THEMES.indexOf(current) + 1) % THEMES.length];
  applyTheme(next);
  announce("Theme set to " + THEME_NAMES[next]);
}

let reloadStatusTimer = null;
function flashReloadStatus(msg) {
  ui.reloadStatus.textContent = msg;
  ui.reloadStatus.classList.add("show");
  clearTimeout(reloadStatusTimer);
  reloadStatusTimer = setTimeout(() => {
    ui.reloadStatus.classList.remove("show");
  }, 2000);
}

async function postReload() {
  ui.reload.disabled = true;
  try {
    const resp = await fetch("api/reload", { method: "POST" });
    if (resp.ok) {
      flashReloadStatus("config reloaded");
      announce("Configuration reloaded");
    } else {
      const body = await resp.json().catch(() => null);
      showError(body && body.error ? body.error.message : "Reload failed.");
    }
  } catch (err) {
    showError("Reload failed: " + err.message);
  } finally {
    ui.reload.disabled = false;
  }
}

// ---- Error handling ----

function showError(message) {
  ui.errorMessage.textContent = message;
  ui.errorPanel.hidden = false;
  announce("Error: " + message);
}

function hideError() {
  ui.errorPanel.hidden = true;
}

// ---- Data flow ----

function setData(data) {
  state.data = data;
  if (!state.loaded) {
    state.loaded = true;
    clearSkeletons();
  }
  if (state.selected && !data.routes.some((r) => r.name === state.selected)) {
    state.selected = null;
  }
  renderHeader();
  renderRoutes();
  renderUpstreams();
  renderLimiters();
  pushHistory(data);
  drawChart();
  if (!ui.detail.hidden) openDetail();
}

function renderHeader() {
  const d = state.data;
  ui.version.textContent = d.version ? "v" + d.version : "";
  ui.uptime.textContent = fmtUptime(d.uptime_seconds || 0);
}

async function fetchState() {
  try {
    const resp = await fetch("api/state", { headers: { Accept: "application/json" } });
    if (!resp.ok) {
      showError("Failed to load state (HTTP " + resp.status + ").");
      return;
    }
    const data = await resp.json();
    hideError();
    setData(data);
  } catch (err) {
    showError("Failed to load state: " + err.message);
  }
}

function connectSSE() {
  if (state.source) return;
  const es = new EventSource("events");
  state.source = es;
  es.addEventListener("state", (ev) => {
    try {
      hideError();
      setData(JSON.parse(ev.data));
    } catch (err) {
      // Malformed frame: keep the last good snapshot on screen.
    }
  });
  es.addEventListener("hello", () => {});
  es.onerror = () => {
    if (es.readyState === EventSource.CLOSED) {
      state.source = null;
      showError("Live update stream lost.");
    }
  };
}

// ---- Keyboard ----

function isTypingTarget(target) {
  return target instanceof HTMLElement
    && (target.tagName === "INPUT" || target.tagName === "TEXTAREA"
      || target.tagName === "SELECT" || target.isContentEditable);
}

function onKeyDown(e) {
  if (e.defaultPrevented) return;
  const typing = isTypingTarget(e.target);
  if (e.key === "Escape") {
    if (closeOverlay() || closeDetail()) {
      e.preventDefault();
    } else if (typing) {
      e.target.blur();
    }
    return;
  }
  if (typing) return;
  switch (e.key) {
    case "?":
      e.preventDefault();
      ui.overlay.hidden = !ui.overlay.hidden;
      if (!ui.overlay.hidden) {
        ui.overlayClose.focus();
        announce("Keyboard shortcuts shown");
      }
      break;
    case "/":
      e.preventDefault();
      ui.filter.focus();
      break;
    case "j":
      e.preventDefault();
      moveSelection(1);
      break;
    case "k":
      e.preventDefault();
      moveSelection(-1);
      break;
    case "Enter": {
      const row = e.target instanceof HTMLElement
        ? e.target.closest("tr.row") : null;
      if (row) selectRoute(row.dataset.key, false);
      else if (state.selected) openDetail();
      e.preventDefault();
      break;
    }
    default:
      break;
  }
}

// ---- Boot ----

function boot() {
  applyTheme(document.documentElement.dataset.theme || "dark");
  skeletons(ui.routesBody, 7, 5);
  skeletons(ui.upstreamsBody, 6, 3);
  skeletons(ui.limitersBody, 9, 3);

  ui.themeBtn.addEventListener("click", cycleTheme);
  ui.helpBtn.addEventListener("click", () => {
    ui.overlay.hidden = false;
    ui.overlayClose.focus();
  });
  ui.overlayClose.addEventListener("click", closeOverlay);
  ui.overlay.addEventListener("click", (e) => {
    if (e.target === ui.overlay) closeOverlay();
  });
  ui.detailClose.addEventListener("click", closeDetail);
  ui.reload.addEventListener("click", postReload);
  ui.btnRetry.addEventListener("click", () => {
    hideError();
    fetchState().then(() => {
      if (state.loaded) connectSSE();
    });
  });
  ui.filter.addEventListener("input", () => {
    state.filterText = ui.filter.value.trim().toLowerCase();
    renderRoutes();
  });

  document.addEventListener("keydown", onKeyDown);
  window.addEventListener("resize", drawChart);

  fetchState().then(() => {
    if (state.loaded) connectSSE();
  });
}

boot();
})();
