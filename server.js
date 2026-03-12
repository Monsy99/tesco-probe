"use strict";

/**
 * multi-monitor/server.js
 *
 * Background multi-endpoint probe service.
 * Each target runs its own independent async probe loop.
 * All data lives in-memory (circular buffers, no DB).
 *
 * Usage:
 *   node server.js [--port <port>]
 *
 * Seed URLs at startup via env (comma-separated):
 *   PROBE_URLS="https://a.com,https://b.com" node server.js
 *
 * REST API:
 *   GET    /api/targets              — list all targets + summary
 *   POST   /api/targets              — add { url, intervalSec?, label? }
 *   DELETE /api/targets/:id          — remove target
 *   POST   /api/targets/:id/start    — start probing
 *   POST   /api/targets/:id/stop     — stop probing
 *   GET    /api/targets/:id/data     — full history for one target
 *   DELETE /api/targets/:id/data     — clear history for one target
 *   POST   /api/targets/probe-all    — trigger immediate probe on all running targets
 *   GET    /                         — dashboard HTML
 */

const http = require("http");
const https = require("https");
const crypto = require("crypto");
const express = require("express");

// ─── Constants ───────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT ?? "3000", 10);
const DEFAULT_INTERVAL_S = 180; // 3 minutes
const MAX_POINTS = 480; // 24 h at 3-min cadence
const PROBE_TIMEOUT_MS = 10_000;

// ─── In-memory store ─────────────────────────────────────────────────────────
// targets: Map<id, Target>
//
// Target = {
//   id          : string          — short random hex id
//   url         : string
//   label       : string
//   intervalSec : number
//   running     : boolean
//   probing     : boolean         — true while a probe is in-flight
//   addedAt     : number          — ms epoch
//   startedAt   : number | null
//   timer       : NodeJS.Timeout | null
//   stopTimer   : NodeJS.Timeout | null
//   history     : Entry[]         — circular buffer, max MAX_POINTS
// }
//
// Entry = { ts, latency, status, ok, error? }

const targets = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────
const uid = () => crypto.randomBytes(4).toString("hex");
const log = (id, msg) => {
  const ts = new Date().toISOString().replace("T", " ").slice(0, 19);
  process.stdout.write(`[${ts}] [${id}] ${msg}\n`);
};
const logSys = (msg) => log("sys", msg);

function bufferPush(history, entry) {
  if (history.length >= MAX_POINTS) history.shift();
  history.push(entry);
}

function summarise(t) {
  const last = t.history[t.history.length - 1] ?? null;
  const upCount = t.history.filter((d) => d.ok).length;
  const latencies = t.history
    .filter((d) => d.latency != null)
    .map((d) => d.latency);
  return {
    id: t.id,
    url: t.url,
    label: t.label,
    intervalSec: t.intervalSec,
    running: t.running,
    probing: t.probing,
    addedAt: t.addedAt,
    startedAt: t.startedAt,
    totalProbes: t.history.length,
    availability: t.history.length
      ? Math.round((upCount / t.history.length) * 100)
      : null,
    avgLatency: latencies.length
      ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length)
      : null,
    lastProbe: last,
  };
}

// ─── Core probe ──────────────────────────────────────────────────────────────
async function probeOnce(t) {
  if (t.probing) return; // skip — previous probe still in-flight
  t.probing = true;

  const t0 = Date.now();
  let entry;

  try {
    const lib = t.url.startsWith("https") ? https : http;
    const statusCode = await new Promise((resolve, reject) => {
      const req = lib.get(t.url, { timeout: PROBE_TIMEOUT_MS }, (res) => {
        res.resume(); // drain body — avoids socket leak
        res.on("end", () => resolve(res.statusCode));
      });
      req.on("timeout", () => {
        req.destroy();
        reject(new Error("timeout"));
      });
      req.on("error", reject);
    });

    entry = {
      ts: t0,
      latency: Date.now() - t0,
      status: statusCode,
      ok: statusCode >= 200 && statusCode < 400,
    };
  } catch (err) {
    entry = {
      ts: t0,
      latency: null,
      status: null,
      ok: false,
      error: err.message.slice(0, 120),
    };
  }

  bufferPush(t.history, entry);
  t.probing = false;
  log(
    t.id,
    `${entry.ok ? "UP  " : "DOWN"} ${entry.latency ?? "—"}ms ${entry.status ?? entry.error}`,
  );
}

// ─── Scheduler ───────────────────────────────────────────────────────────────
function startTarget(t) {
  if (t.running) return { ok: false, message: "Already running" };

  t.running = true;
  t.startedAt = Date.now();

  probeOnce(t); // immediate first probe (async, fire-and-forget)

  t.timer = setInterval(() => probeOnce(t), t.intervalSec * 1000);

  // Auto-stop after 24 h
  t.stopTimer = setTimeout(
    () => {
      logSys(`auto-stop 24h: ${t.id}`);
      stopTarget(t);
    },
    24 * 60 * 60 * 1000,
  );

  log(t.id, `started  url=${t.url}  interval=${t.intervalSec}s`);
  return { ok: true };
}

function stopTarget(t) {
  if (!t.running) return { ok: false, message: "Not running" };
  clearInterval(t.timer);
  clearTimeout(t.stopTimer);
  t.timer = null;
  t.stopTimer = null;
  t.running = false;
  log(t.id, "stopped");
  return { ok: true };
}

function removeTarget(id) {
  const t = targets.get(id);
  if (!t) return false;
  stopTarget(t); // clean up timers before removing
  targets.delete(id);
  logSys(`removed ${id}`);
  return true;
}

function addTarget({ url, label, intervalSec, autoStart = true }) {
  if (!url || !url.startsWith("http")) throw new Error("Invalid URL");
  const t = {
    id: uid(),
    url: url.trim(),
    label: (label ?? url).trim(),
    intervalSec: Math.max(10, parseInt(intervalSec ?? DEFAULT_INTERVAL_S, 10)),
    running: false,
    probing: false,
    addedAt: Date.now(),
    startedAt: null,
    timer: null,
    stopTimer: null,
    history: [],
  };
  targets.set(t.id, t);
  logSys(`added ${t.id}  ${t.url}`);
  if (autoStart) startTarget(t);
  return t;
}

// ─── Express ─────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  if (req.path.startsWith("/api")) logSys(`${req.method} ${req.path}`);
  next();
});

// List all targets
app.get("/api/targets", (_req, res) => {
  res.json([...targets.values()].map(summarise));
});

// Add target
app.post("/api/targets", (req, res) => {
  try {
    const t = addTarget(req.body ?? {});
    res.status(201).json(summarise(t));
  } catch (e) {
    res.status(400).json({ ok: false, message: e.message });
  }
});

// Remove target
app.delete("/api/targets/:id", (req, res) => {
  const ok = removeTarget(req.params.id);
  res.json({ ok, message: ok ? "Removed" : "Not found" });
});

// Start / stop
app.post("/api/targets/:id/start", (req, res) => {
  const t = targets.get(req.params.id);
  if (!t) return res.status(404).json({ ok: false, message: "Not found" });
  res.json(startTarget(t));
});
app.post("/api/targets/:id/stop", (req, res) => {
  const t = targets.get(req.params.id);
  if (!t) return res.status(404).json({ ok: false, message: "Not found" });
  res.json(stopTarget(t));
});

// History for one target
app.get("/api/targets/:id/data", (req, res) => {
  const t = targets.get(req.params.id);
  if (!t) return res.status(404).json({ ok: false, message: "Not found" });
  res.json({
    id: t.id,
    url: t.url,
    count: t.history.length,
    points: t.history,
  });
});

// Clear history
app.delete("/api/targets/:id/data", (req, res) => {
  const t = targets.get(req.params.id);
  if (!t) return res.status(404).json({ ok: false, message: "Not found" });
  t.history = [];
  res.json({ ok: true });
});

// Trigger immediate probe on all running targets (async — returns immediately)
app.post("/api/targets/probe-all", (_req, res) => {
  const running = [...targets.values()].filter((t) => t.running);
  running.forEach((t) => probeOnce(t)); // fire-and-forget
  res.json({ ok: true, triggered: running.length });
});

// Dashboard
app.get("/", (_req, res) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(DASHBOARD_HTML);
});

// ─── Seed from env ────────────────────────────────────────────────────────────
if (process.env.PROBE_URLS) {
  process.env.PROBE_URLS.split(",")
    .map((u) => u.trim())
    .filter(Boolean)
    .forEach((url) => {
      try {
        addTarget({ url });
      } catch (e) {
        logSys(`seed error: ${e.message}`);
      }
    });
}

// ─── Start server ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  logSys(`listening  http://localhost:${PORT}`);
  logSys(`dashboard  http://localhost:${PORT}/`);
});

// ─── Graceful shutdown ────────────────────────────────────────────────────────
function shutdown(sig) {
  logSys(`${sig} — shutting down`);
  [...targets.values()].forEach(stopTarget);
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ─────────────────────────────────────────────────────────────────────────────
// DASHBOARD HTML — served at GET /
// Pure vanilla JS, Chart.js from CDN. Polls /api/targets every 15 s.
// Per-target sparklines drawn on demand.
// ─────────────────────────────────────────────────────────────────────────────
const DASHBOARD_HTML = /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Multi-Monitor</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#080808;--surface:#0d0d0d;--raised:#141414;--border:#1e1e1e;
  --text:#e0e0e0;--muted:#555;--dim:#2a2a2a;
  --green:#39ff14;--yellow:#f0c040;--red:#ff3b3b;--blue:#7eb8ff;
  --font:'IBM Plex Mono',monospace;
}
body{background:var(--bg);color:var(--text);font-family:var(--font);min-height:100vh;padding:28px 24px;display:flex;flex-direction:column;gap:20px}
h1{font-size:18px;font-weight:700;letter-spacing:-.02em}
.eyebrow{font-size:10px;letter-spacing:.25em;color:var(--muted);text-transform:uppercase;margin-bottom:4px}

/* ── Add-target form ── */
.add-bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.add-bar input{background:#111;border:1px solid var(--border);color:var(--text);
  padding:9px 13px;font:13px var(--font);outline:none;border-radius:2px;flex:1;min-width:180px}
.add-bar input[type=number]{max-width:90px;flex:none}
button{padding:9px 18px;font:700 11px var(--font);letter-spacing:.1em;text-transform:uppercase;
  border-radius:2px;cursor:pointer;background:transparent;transition:opacity .15s}
button:hover{opacity:.75}
.btn-green{border:1px solid var(--green);color:var(--green)}
.btn-red  {border:1px solid var(--red);color:var(--red)}
.btn-dim  {border:1px solid var(--dim);color:var(--muted)}

/* ── Summary bar ── */
#summary{display:flex;gap:20px;flex-wrap:wrap;font-size:11px;color:var(--muted)}
#summary span b{color:#888;font-weight:400}

/* ── Target grid ── */
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}

/* ── Target card ── */
.card{background:var(--surface);border:1px solid var(--border);border-radius:3px;
  overflow:hidden;display:flex;flex-direction:column}
.card-head{display:flex;align-items:center;gap:10px;padding:12px 14px;border-bottom:1px solid var(--border);cursor:pointer}
.card-head:hover{background:var(--raised)}
.dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;transition:background .3s,box-shadow .3s}
.dot.up   {background:var(--green);box-shadow:0 0 7px var(--green)}
.dot.down {background:var(--red);box-shadow:0 0 7px var(--red)}
.dot.idle {background:var(--dim)}
.dot.probe{background:var(--yellow);box-shadow:0 0 7px var(--yellow)}
.card-label{font-size:12px;font-weight:600;flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.card-url  {font-size:10px;color:var(--muted);overflow:hidden;white-space:nowrap;text-overflow:ellipsis;max-width:160px}
.card-avail{font-size:12px;font-weight:700;margin-left:auto;flex-shrink:0}

.card-stats{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;background:var(--border)}
.stat{background:var(--surface);padding:8px 12px}
.stat-label{font-size:8px;letter-spacing:.18em;text-transform:uppercase;color:var(--muted);margin-bottom:3px}
.stat-val  {font-size:14px;font-weight:700}

.card-spark{padding:10px 12px 8px;border-top:1px solid var(--border)}
.card-spark canvas{display:block;height:48px !important}

.card-foot{display:flex;gap:6px;padding:10px 12px;border-top:1px solid var(--border)}
.card-foot button{padding:6px 12px;font-size:10px}

/* ── Detail panel ── */
#detail{background:var(--surface);border:1px solid var(--border);border-radius:3px;display:none;flex-direction:column;gap:0}
#detail-head{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid var(--border)}
#detail-title{font-size:14px;font-weight:700;flex:1}
#detail-charts{padding:16px;display:flex;flex-direction:column;gap:16px}
.chart-wrap{background:var(--raised);border:1px solid var(--border);border-radius:2px;padding:12px 10px 8px}
.chart-title{font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:var(--muted);margin-bottom:8px}
canvas.detail{max-height:160px}
#detail-log{border-top:1px solid var(--border);max-height:200px;overflow-y:auto;padding:10px 16px}
.log-row{font-size:10px;line-height:1.9;border-bottom:1px solid #141414;padding:1px 0}
.log-row.up{color:#7aff7a}.log-row.down{color:#ff6b6b}

/* ── Empty state ── */
#empty{text-align:center;color:var(--dim);padding:60px 0;font-size:13px}

/* ── Scrollbar ── */
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:#111}::-webkit-scrollbar-thumb{background:#2a2a2a}

/* ── Status pulse anim ── */
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.dot.up{animation:blink 2s ease-in-out infinite}
</style>
</head>
<body>
<div>
  <div class="eyebrow">Multi-Endpoint Monitor</div>
  <h1 id="title">● Probe Service</h1>
</div>

<!-- Add form -->
<div class="add-bar">
  <input type="text"   id="newUrl"      placeholder="https://example.com/health"/>
  <input type="text"   id="newLabel"    placeholder="Label (optional)" style="max-width:160px"/>
  <input type="number" id="newInterval" placeholder="180s" min="10" style="max-width:80px"/>
  <button class="btn-green" onclick="addTarget()">+ Add &amp; Start</button>
</div>

<div id="summary"></div>
<div id="grid"></div>
<div id="empty">No targets yet — add a URL above.</div>

<!-- Detail panel -->
<div id="detail">
  <div id="detail-head">
    <span class="dot idle" id="detail-dot"></span>
    <span id="detail-title">—</span>
    <button class="btn-dim" onclick="closeDetail()">✕ Close</button>
  </div>
  <div id="detail-charts">
    <div class="chart-wrap">
      <div class="chart-title">Latency (ms) over time</div>
      <canvas class="detail" id="dtLatency"></canvas>
    </div>
    <div class="chart-wrap">
      <div class="chart-title">Availability (1 = up, 0 = down)</div>
      <canvas class="detail" id="dtAvail"></canvas>
    </div>
  </div>
  <div id="detail-log"></div>
</div>

<script>
const fmt    = ts => new Date(ts).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
const fmtMs  = ms => ms != null ? ms+'ms' : '—';
const $ = id => document.getElementById(id);

let allTargets   = [];          // last fetched summaries
let detailId     = null;        // currently open detail panel
let sparkCharts  = {};          // id → Chart  (sparklines, one per card)
let dtLatChart   = null;        // detail latency chart
let dtAvailChart = null;        // detail avail chart
let pollTimer    = null;

// ── Chart helpers ─────────────────────────────────────────────────────────
const CHART_BASE_OPTS = {
  animation: false,
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { display: false },
    tooltip: {
      backgroundColor:'#0d0d0d',borderColor:'#2a2a2a',borderWidth:1,
      titleColor:'#888',bodyColor:'#e0e0e0',
      titleFont:{family:'IBM Plex Mono',size:10},bodyFont:{family:'IBM Plex Mono',size:10},
    },
  },
  scales: {
    x: { display:false },
    y: { ticks:{color:'#444',font:{family:'IBM Plex Mono',size:9}}, grid:{color:'#1a1a1a'} },
  },
};

function makeSparkChart(canvas, points) {
  return new Chart(canvas, {
    type: 'line',
    data: {
      labels: points.map(d => fmt(d.ts)),
      datasets: [{
        data: points.map(d => d.latency),
        borderColor: '#f0c040', borderWidth: 1.5,
        pointBackgroundColor: points.map(d => d.ok ? '#f0c040' : '#ff3b3b'),
        pointRadius: 2, tension: 0.2, fill: false,
      }],
    },
    options: { ...CHART_BASE_OPTS, animation: false,
      scales: { x:{display:false}, y:{...CHART_BASE_OPTS.scales.y, display:false} } },
  });
}

function makeDetailCharts() {
  const base = { ...CHART_BASE_OPTS,
    scales: {
      x: { ticks:{color:'#444',font:{family:'IBM Plex Mono',size:9},maxRotation:0,autoSkipPadding:20}, grid:{color:'#1a1a1a'} },
      y: { ...CHART_BASE_OPTS.scales.y },
    },
  };
  dtLatChart   = new Chart($('dtLatency'), {
    type:'line',
    data:{ labels:[], datasets:[{ data:[],borderColor:'#f0c040',borderWidth:1.5,
      pointBackgroundColor:[],pointRadius:3,tension:0.2,fill:false }] },
    options: base,
  });
  dtAvailChart = new Chart($('dtAvail'), {
    type:'line',
    data:{ labels:[], datasets:[{ data:[],borderColor:'#39ff14',borderWidth:2,
      pointRadius:0,stepped:'after',fill:false }] },
    options: { ...base, scales:{ ...base.scales, y:{ ...base.scales.y, min:-0.1, max:1.1,
      ticks:{...base.scales.y.ticks,stepSize:1} } } },
  });
}

function updateDetailCharts(points) {
  const labels  = points.map(d => fmt(d.ts));
  const latency = points.map(d => d.latency);
  const avail   = points.map(d => d.ok ? 1 : 0);
  const colors  = points.map(d => d.ok ? '#f0c040' : '#ff3b3b');
  dtLatChart.data.labels = dtAvailChart.data.labels = labels;
  dtLatChart.data.datasets[0].data = latency;
  dtLatChart.data.datasets[0].pointBackgroundColor = colors;
  dtAvailChart.data.datasets[0].data = avail;
  dtLatChart.update('none');
  dtAvailChart.update('none');
}

// ── Render summary bar ─────────────────────────────────────────────────────
function renderSummary(ts) {
  const total   = ts.length;
  const running = ts.filter(t => t.running).length;
  const up      = ts.filter(t => t.lastProbe?.ok).length;
  const down    = ts.filter(t => t.lastProbe && !t.lastProbe.ok).length;
  $('summary').innerHTML = [
    \`targets <b>\${total}</b>\`,
    \`running <b>\${running}</b>\`,
    up   ? \`<span style="color:#39ff14">up <b>\${up}</b></span>\` : '',
    down ? \`<span style="color:#ff3b3b">down <b>\${down}</b></span>\` : '',
  ].filter(Boolean).map(s => \`<span>\${s}</span>\`).join('');
}

// ── Render target cards ────────────────────────────────────────────────────
function dotClass(t) {
  if (t.probing)              return 'probe';
  if (!t.running)             return 'idle';
  if (t.lastProbe?.ok)        return 'up';
  if (t.lastProbe && !t.lastProbe.ok) return 'down';
  return 'idle';
}

function availColor(a) {
  if (a == null) return '#555';
  if (a >= 95)   return '#39ff14';
  if (a >= 80)   return '#f0c040';
  return '#ff3b3b';
}

function renderGrid(ts) {
  const grid = $('grid');
  $('empty').style.display = ts.length ? 'none' : '';
  grid.style.display       = ts.length ? '' : 'none';

  // Build a set of current ids to detect removed cards
  const existingCards = new Set([...grid.querySelectorAll('.card')].map(el => el.dataset.id));
  const newIds        = new Set(ts.map(t => t.id));

  // Remove stale cards
  existingCards.forEach(id => {
    if (!newIds.has(id)) {
      grid.querySelector(\`[data-id="\${id}"]\`)?.remove();
      if (sparkCharts[id]) { sparkCharts[id].destroy(); delete sparkCharts[id]; }
    }
  });

  ts.forEach(t => {
    const dc = dotClass(t);
    const ac = availColor(t.availability);
    const lp = t.lastProbe;

    let card = grid.querySelector(\`[data-id="\${t.id}"]\`);
    if (!card) {
      card = document.createElement('div');
      card.className = 'card';
      card.dataset.id = t.id;
      card.innerHTML = \`
        <div class="card-head" onclick="openDetail('\${t.id}')">
          <span class="dot \${dc}" id="dot-\${t.id}"></span>
          <div style="flex:1;overflow:hidden">
            <div class="card-label" id="lbl-\${t.id}">\${esc(t.label)}</div>
            <div class="card-url">\${esc(t.url)}</div>
          </div>
          <div class="card-avail" id="avail-\${t.id}" style="color:\${ac}">\${t.availability != null ? t.availability+'%' : '—'}</div>
        </div>
        <div class="card-stats" id="cs-\${t.id}">
          \${statsHtml(t)}
        </div>
        <div class="card-spark">
          <canvas id="spark-\${t.id}" height="48"></canvas>
        </div>
        <div class="card-foot">
          \${footHtml(t)}
        </div>\`;
      grid.appendChild(card);
    } else {
      // Update mutable parts only — avoid full re-render flicker
      const dotEl = $(\`dot-\${t.id}\`);
      if (dotEl) dotEl.className = \`dot \${dc}\`;
      const availEl = $(\`avail-\${t.id}\`);
      if (availEl) { availEl.textContent = t.availability != null ? t.availability+'%' : '—'; availEl.style.color = ac; }
      const csEl = $(\`cs-\${t.id}\`);
      if (csEl) csEl.innerHTML = statsHtml(t);
      const footEl = card.querySelector('.card-foot');
      if (footEl) footEl.innerHTML = footHtml(t);
    }
  });
}

function statsHtml(t) {
  const lp = t.lastProbe;
  return [
    { label:'Status',    val: lp ? (lp.ok?'UP':'DOWN') : '—',
      color: lp ? (lp.ok?'#39ff14':'#ff3b3b') : '#555' },
    { label:'Latency',   val: lp?.latency != null ? lp.latency+'ms' : '—', color:'#f0c040' },
    { label:'Avg Lat',   val: t.avgLatency != null ? t.avgLatency+'ms' : '—', color:'#f0c040' },
  ].map(s => \`<div class="stat"><div class="stat-label">\${s.label}</div>
    <div class="stat-val" style="color:\${s.color}">\${s.val}</div></div>\`).join('');
}

function footHtml(t) {
  const start = \`<button class="btn-green" onclick="startTarget('\${t.id}',event)">▶ Start</button>\`;
  const stop  = \`<button class="btn-red"   onclick="stopTarget('\${t.id}',event)">■ Stop</button>\`;
  const del   = \`<button class="btn-dim"   onclick="removeTarget('\${t.id}',event)">✕ Remove</button>\`;
  const clr   = \`<button class="btn-dim"   onclick="clearData('\${t.id}',event)">⌫ Clear</button>\`;
  return (t.running ? stop : start) + clr + del;
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Sparklines (drawn after cards are in DOM) ──────────────────────────────
async function refreshSparklines(ts) {
  for (const t of ts) {
    const canvas = $(\`spark-\${t.id}\`);
    if (!canvas) continue;
    // Fetch history lazily for sparkline
    try {
      const res    = await fetch(\`/api/targets/\${t.id}/data\`);
      const { points } = await res.json();
      if (!points.length) continue;
      if (sparkCharts[t.id]) {
        // Update in-place
        const ch = sparkCharts[t.id];
        ch.data.labels = points.map(d => fmt(d.ts));
        ch.data.datasets[0].data               = points.map(d => d.latency);
        ch.data.datasets[0].pointBackgroundColor = points.map(d => d.ok ? '#f0c040' : '#ff3b3b');
        ch.update('none');
      } else {
        sparkCharts[t.id] = makeSparkChart(canvas, points);
      }
    } catch(_) {}
  }
}

// ── Detail panel ───────────────────────────────────────────────────────────
async function openDetail(id) {
  detailId = id;
  const t = allTargets.find(x => x.id === id);
  if (!t) return;

  $('detail-title').textContent = t.label + '  ' + t.url;
  $('detail-dot').className = 'dot ' + dotClass(t);
  $('detail').style.display = 'flex';

  if (!dtLatChart) makeDetailCharts();

  const res    = await fetch(\`/api/targets/\${id}/data\`);
  const { points } = await res.json();
  updateDetailCharts(points);
  renderDetailLog(points);
}

function closeDetail() {
  detailId = null;
  $('detail').style.display = 'none';
}

function renderDetailLog(points) {
  const rows = [...points].reverse().slice(0, 100);
  $('detail-log').innerHTML = rows.map(d => \`
    <div class="log-row \${d.ok?'up':'down'}">
      <span style="color:#555">\${fmt(d.ts)}</span>
      &nbsp;&nbsp;\${d.ok?'✓ UP':'✗ DOWN'}
      &nbsp;&nbsp;\${d.latency!=null?'<span style="color:#888">'+d.latency+'ms</span>':''}
      &nbsp;&nbsp;<span style="color:#444">\${d.status ?? d.error ?? ''}</span>
    </div>\`).join('');
}

// ── Controls ───────────────────────────────────────────────────────────────
async function addTarget() {
  const url      = $('newUrl').value.trim();
  const label    = $('newLabel').value.trim() || undefined;
  const interval = parseInt($('newInterval').value) || undefined;
  if (!url) { alert('Enter a URL'); return; }
  const res = await fetch('/api/targets', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ url, label, intervalSec: interval }),
  });
  const j = await res.json();
  if (!j.id) { alert(j.message); return; }
  $('newUrl').value = $('newLabel').value = $('newInterval').value = '';
  await loadAll();
}

async function startTarget(id, e) {
  e?.stopPropagation();
  await fetch(\`/api/targets/\${id}/start\`, { method:'POST' });
  loadAll();
}
async function stopTarget(id, e) {
  e?.stopPropagation();
  await fetch(\`/api/targets/\${id}/stop\`, { method:'POST' });
  loadAll();
}
async function removeTarget(id, e) {
  e?.stopPropagation();
  if (!confirm('Remove this target?')) return;
  await fetch(\`/api/targets/\${id}\`, { method:'DELETE' });
  if (detailId === id) closeDetail();
  loadAll();
}
async function clearData(id, e) {
  e?.stopPropagation();
  await fetch(\`/api/targets/\${id}/data\`, { method:'DELETE' });
  loadAll();
}

// ── Main data load ─────────────────────────────────────────────────────────
async function loadAll() {
  try {
    const res = await fetch('/api/targets');
    allTargets = await res.json();
    renderSummary(allTargets);
    renderGrid(allTargets);
    refreshSparklines(allTargets); // async, doesn't block render
    // Refresh open detail panel
    if (detailId) openDetail(detailId);
  } catch(e) { console.error(e); }
}

// ── Polling ────────────────────────────────────────────────────────────────
function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(loadAll, 15_000);
}
document.addEventListener('visibilitychange', () => {
  if (document.hidden) { clearInterval(pollTimer); pollTimer = null; }
  else { loadAll(); startPolling(); }
});

loadAll();
startPolling();
</script>
</body>
</html>`;
