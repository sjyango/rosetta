"""HTML benchmark report generator for Rosetta.

Generates a self-contained HTML file with:
- Dashboard cards (total queries, QPS, duration)
- Per-DBMS latency stats table with tabs
- Grouped bar chart (ECharts) for cross-DBMS comparison
- Expandable raw latency data
"""

import html
import json
import logging
import time

from ..models import BenchmarkResult, DBMSBenchResult, QueryLatencyStats

log = logging.getLogger("rosetta")


def _escape(text: str) -> str:
    return html.escape(text, quote=True)


def _build_data(result: BenchmarkResult) -> dict:
    """Convert BenchmarkResult into a JSON-serialisable dict."""
    dbms_list = []
    for dr in result.dbms_results:
        queries = []
        for qs in dr.query_stats:
            queries.append({
                "name": qs.query_name,
                "sql": qs.sql_template,
                "exec": qs.total_executions,
                "errors": qs.total_errors,
                "avg": round(qs.avg_ms, 3),
                "p50": round(qs.p50_ms, 3),
                "p95": round(qs.p95_ms, 3),
                "p99": round(qs.p99_ms, 3),
                "min": round(qs.min_ms, 3),
                "max": round(qs.max_ms, 3),
                "qps": round(qs.qps, 1),
                "has_flamegraph": bool(qs.flamegraph_svg),
                "explain": qs.explain_plan or "",
                "explain_tree": qs.explain_tree or "",
            })
        dbms_list.append({
            "name": dr.dbms_name,
            "overall_qps": round(dr.overall_qps, 1),
            "total_duration": round(dr.total_duration_s, 2),
            "total_queries": dr.total_queries,
            "total_errors": dr.total_errors,
            "queries": queries,
        })
    return {
        "workload": result.workload_name,
        "mode": result.mode.name,
        "iterations": result.config.iterations,
        "warmup": result.config.warmup,
        "concurrency": result.config.concurrency,
        "duration": result.config.duration,
        "ramp_up": result.config.ramp_up,
        "timestamp": result.timestamp or time.strftime("%Y-%m-%d %H:%M:%S"),
        "table_rows": result.table_rows,
        "table_rows_detail": result.table_rows_detail or {},
        "dbms": dbms_list,
        "has_profile": result.config.profile,
        "setup_sql": list(result.setup_sql) if result.setup_sql else [],
        "teardown_sql": list(result.teardown_sql) if result.teardown_sql else [],
        "queries_sql": list(result.queries_sql) if result.queries_sql else [],
    }


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta Benchmark — {{WORKLOAD}}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --red: #f85149; --blue: #58a6ff;
  --yellow: #d29922; --orange: #db8b0b; --purple: #a371f7;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box;
  scrollbar-width: thin; scrollbar-color: var(--bg3) var(--bg); }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: var(--bg); border-radius: 6px; }
::-webkit-scrollbar-thumb { background: var(--bg3); border-radius: 6px;
  border: 2px solid var(--bg); }
::-webkit-scrollbar-thumb:hover { background: var(--fg2); }
::-webkit-scrollbar-corner { background: var(--bg); }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 24px; }
h2 { font-size: 18px; margin-bottom: 16px; color: var(--fg); }
.meta { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }
.meta span { margin-right: 16px; }

/* Config panel */
.config-panel { background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
  padding: 20px 24px; margin-bottom: 24px; }
.config-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 12px 24px; }
.config-item { display: flex; flex-direction: column; gap: 2px; }
.config-item .cfg-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px;
  color: var(--fg2); font-weight: 600; }
.config-item .cfg-value { font-size: 15px; font-weight: 600; color: var(--fg); }
.config-item .cfg-value.highlight { color: var(--blue); }
.config-item .cfg-value .badge-on { display: inline-block; padding: 1px 8px; border-radius: 4px;
  font-size: 12px; font-weight: 600; background: #b91c1c; color: #fff; }
.config-item .cfg-value .badge-off { display: inline-block; padding: 1px 8px; border-radius: 4px;
  font-size: 12px; font-weight: 600; background: var(--bg3); color: var(--fg2); }
.config-item .cfg-value .mode-serial { color: var(--green); }
.config-item .cfg-value .mode-concurrent { color: var(--orange); }
.table-rows-wrap { margin-top: 6px; display: flex; flex-wrap: wrap; gap: 6px; }
.table-chip { display: inline-flex; align-items: center; gap: 6px; background: var(--bg);
  border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px; font-size: 12px;
  line-height: 1.4; transition: border-color 0.15s; }
.table-chip:hover { border-color: var(--fg2); }
.table-chip .tc-name { color: var(--blue); font-family: 'SF Mono', Consolas, monospace;
  font-size: 11px; font-weight: 500; }
.table-chip .tc-count { color: var(--fg); font-weight: 600; font-size: 12px; }
.table-chip .tc-sep { color: var(--border); }

/* Cards */
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px; margin-bottom: 24px; }
.card { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
  padding: 16px; }
.card .label { font-size: 12px; color: var(--fg2); text-transform: uppercase;
  letter-spacing: 0.5px; margin-bottom: 4px; }
.card .value { font-size: 24px; font-weight: 700; }
.card .sub { font-size: 12px; color: var(--fg2); margin-top: 2px; }

/* Section */
.section { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
  padding: 20px; margin-bottom: 20px; }

/* Tabs */
.tabs { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
.tab { padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 14px;
  border: 1px solid var(--border); background: var(--bg3); color: var(--fg2);
  transition: all 0.15s; user-select: none; }
.tab:hover { color: var(--fg); border-color: var(--fg2); }
.tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }

/* Table */
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th { text-align: left; padding: 8px 12px; border-bottom: 2px solid var(--border);
  color: var(--fg2); font-weight: 600; position: sticky; top: 0; background: var(--bg2); }
th.num { text-align: right; }
td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
td.num { text-align: right; font-family: 'SF Mono', Consolas, monospace; font-size: 13px; }
tr:hover { background: var(--bg3); }
.qname { font-weight: 600; color: var(--blue); cursor: pointer; }
.qname:hover { text-decoration: underline; }
.sql-row td { padding: 4px 12px 12px; border-bottom: 1px solid var(--border); }
.sql-code { font-family: 'SF Mono', Consolas, monospace; font-size: 12px; color: var(--fg2);
  background: var(--bg3); border-radius: 6px; padding: 8px 12px; white-space: pre-wrap;
  word-break: break-all; line-height: 1.6; }
.dbms-tag { display:inline-block; padding:2px 8px; border-radius:4px; font-size:12px;
  font-weight:600; color:#fff; white-space:nowrap; }
.query-group-first td { border-top: 2px solid var(--border); }
.query-group td { border-bottom-color: rgba(48,54,61,0.4); }

/* Query detail selector */
.q-dropdown-wrap { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
.q-dropdown-wrap label { font-size: 14px; font-weight: 600; color: var(--fg2); white-space: nowrap; }
.q-dropdown { appearance: none; background: var(--bg3); border: 1px solid var(--border);
  border-radius: 6px; padding: 8px 36px 8px 14px; font-size: 14px; color: var(--fg);
  cursor: pointer; min-width: 240px;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%238b949e' stroke-width='1.5' fill='none'/%3E%3C/svg%3E");
  background-repeat: no-repeat; background-position: right 12px center;
  transition: border-color 0.15s; }
.q-dropdown:hover { border-color: var(--fg2); }
.q-dropdown:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px rgba(31,111,235,0.2); }
.q-dropdown option { background: var(--bg2); color: var(--fg); }

/* Query detail panel */
.q-detail-panel { border: 1px solid var(--border); border-radius: 8px; overflow: hidden;
  background: var(--bg2); }
.q-empty { padding: 40px; text-align: center; color: var(--fg2); font-size: 14px; }
.q-sub { padding: 16px; }
.q-sub-title { font-size: 14px; font-weight: 600; color: var(--fg); margin-bottom: 10px;
  display: flex; align-items: center; gap: 8px; }
.q-sub-title .q-icon { font-size: 16px; }
.q-separator { border: none; border-top: 1px solid var(--border); margin: 0; }

/* SQL code block */
.sql-code { font-family: 'SF Mono', Consolas, monospace; font-size: 12px; color: var(--fg2);
  background: var(--bg); border-radius: 6px; padding: 8px 12px; white-space: pre-wrap;
  word-break: break-all; line-height: 1.6; border: 1px solid var(--border); }

/* Schema (setup SQL) */
.schema-block { margin-bottom: 10px; }
.schema-toggle { display: inline-flex; align-items: center; gap: 6px; cursor: pointer;
  font-size: 12px; color: var(--fg2); padding: 4px 10px; border-radius: 6px;
  border: 1px solid var(--border); background: var(--bg3); user-select: none;
  transition: all 0.15s; }
.schema-toggle:hover { color: var(--fg); border-color: var(--fg2); }
.schema-toggle .arrow { transition: transform 0.2s; font-size: 10px; }
.schema-toggle.open .arrow { transform: rotate(90deg); }
.schema-sql { font-family: 'SF Mono', Consolas, monospace; font-size: 12px; color: var(--fg2);
  background: var(--bg); border-radius: 6px; padding: 12px 16px; white-space: pre-wrap;
  word-break: break-all; line-height: 1.8; border: 1px solid var(--border); margin-top: 8px; }
.schema-sql .kw { color: var(--purple); font-weight: 600; }
.schema-sql .type { color: var(--green); }
.schema-sql .name { color: var(--blue); }
.schema-sql .num { color: var(--orange); }
.schema-sql .paren { color: var(--fg2); }
.schema-sql .str { color: var(--yellow); }
.schema-count { font-size: 11px; color: var(--fg2); background: var(--bg);
  padding: 1px 8px; border-radius: 10px; }

/* EXPLAIN plan */
.q-explain-wrap { margin-bottom: 8px; }
.q-explain-label { font-size: 12px; font-weight: 600; margin-bottom: 4px; display: flex;
  align-items: center; gap: 6px; }
.q-explain { font-family: 'SF Mono', Consolas, monospace; font-size: 11px; color: var(--fg2);
  background: var(--bg); border-radius: 6px; padding: 10px 12px; white-space: pre;
  overflow-x: auto; line-height: 1.5; border: 1px solid var(--border); margin-bottom: 8px; }
.q-explain-tree { font-size: 12px; line-height: 1.7; }

/* Chart container */
.chart-box { width: 100%; height: 480px; }

/* Overall QPS bar */
.chart-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
.chart-label { min-width: 120px; font-size: 14px; font-weight: 600; text-align: right; }
.chart-bar-bg { flex: 1; height: 28px; background: var(--bg3); border-radius: 6px;
  overflow: hidden; position: relative; }
.chart-bar { height: 100%; border-radius: 6px; display: flex; align-items: center;
  padding: 0 12px; font-size: 13px; font-weight: 600; color: #fff;
  transition: width 0.6s ease; min-width: fit-content; }
.chart-val { min-width: 80px; font-size: 13px; color: var(--fg2); }
.c0 { background: linear-gradient(90deg, #2563eb, #3b82f6); }
.c1 { background: linear-gradient(90deg, #059669, #10b981); }
.c2 { background: linear-gradient(90deg, #d97706, #f59e0b); }
.c3 { background: linear-gradient(90deg, #7c3aed, #8b5cf6); }
.c4 { background: linear-gradient(90deg, #dc2626, #ef4444); }

/* Responsive */
@media (max-width: 768px) {
  .chart-box { height: 360px; }
  .cards { grid-template-columns: repeat(2, 1fr); }
}

/* Flame Graph section */
.fg-section { margin-bottom: 20px; }
.fg-nav { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
.fg-nav-item { padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 14px;
  border: 1px solid var(--border); background: var(--bg3); color: var(--fg2);
  transition: all 0.15s; user-select: none; }
.fg-nav-item:hover { color: var(--fg); border-color: var(--fg2); }
.fg-nav-item.active { background: #b91c1c; color: #fff; border-color: #b91c1c; }
.fg-container { width: 100%; overflow-x: auto; background: var(--bg); border-radius: 6px;
  border: 1px solid var(--border); }
.fg-container svg { width: 100%; height: auto; display: block; }
.fg-container .fg-frame rect,
.fg-container .fg-frame text { transition: x 0.25s ease, width 0.25s ease; }
.fg-empty { padding: 40px; text-align: center; color: var(--fg2); font-size: 14px; }
.fg-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px;
  font-weight: 600; background: #b91c1c; color: #fff; margin-left: 8px; vertical-align: middle; }
#fg-tooltip { position: fixed; display: none; background: #161b22; border: 1px solid #30363d;
  border-radius: 6px; padding: 8px 12px; color: #e6edf3;
  font: 12px/1.5 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
  pointer-events: none; z-index: 9999; white-space: nowrap; max-width: 600px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.4); }
#fg-tooltip .tt-name { font-weight: 600; margin-bottom: 2px; white-space: normal; word-break: break-all; }
#fg-tooltip .tt-info { color: #8b949e; font-size: 11px; }
</style>
</head>
<body>
<div id="fg-tooltip"><div class="tt-name"></div><div class="tt-info"></div></div>
<div class="container">
  <div style="display:flex;align-items:center;gap:14px;margin-bottom:16px">
    <svg width="44" height="44" viewBox="0 0 44 44" fill="none" xmlns="http://www.w3.org/2000/svg">
      <!-- Stone tablet shape -->
      <rect x="4" y="2" width="36" height="40" rx="4" ry="12" fill="#161b22" stroke="#30363d" stroke-width="1.5"/>
      <!-- Three bar columns representing benchmark comparison -->
      <rect x="10" y="22" width="6" height="14" rx="1.5" fill="#3b82f6" opacity="0.9"/>
      <rect x="19" y="14" width="6" height="22" rx="1.5" fill="#10b981" opacity="0.9"/>
      <rect x="28" y="18" width="6" height="18" rx="1.5" fill="#f59e0b" opacity="0.9"/>
      <!-- Top decorative lines (Rosetta stone inscriptions) -->
      <line x1="10" y1="7" x2="34" y2="7" stroke="#30363d" stroke-width="1.2" stroke-linecap="round"/>
      <line x1="10" y1="10" x2="28" y2="10" stroke="#30363d" stroke-width="1.2" stroke-linecap="round"/>
    </svg>
    <h1>Rosetta Benchmark Report</h1>
    <a href="../index.html" style="color:var(--blue);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9664; History</a>
  </div>
  <div class="config-panel" id="config-panel"></div>

  <!-- Dashboard cards -->
  <div class="cards" id="cards"></div>

  <!-- Main chart: grouped bar chart via ECharts -->
  <div class="section">
    <h2>Cross-DBMS Query Comparison</h2>
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
      <span style="font-size:13px;color:var(--fg2)">Metric:</span>
      <div class="tabs" id="chart-metric-tabs" style="margin-bottom:0"></div>
    </div>
    <div id="main-chart" class="chart-box"></div>
  </div>

  <!-- Overall QPS comparison -->
  <div class="section">
    <h2>Overall QPS</h2>
    <div id="qps-chart"></div>
  </div>

  <!-- Workload Definition (Setup / Queries / Teardown) -->
  <div class="section" id="schema-section" style="display:none">
    <h2>&#128220; Workload Definition</h2>
    <div id="schema-content"></div>
  </div>

  <!-- Per-Query detail dropdown -->
  <div class="section">
    <h2>Per-Query Latency Details</h2>
    <div class="q-dropdown-wrap">
      <label for="query-select">Query:</label>
      <select id="query-select" class="q-dropdown">
        <option value="">-- Select a query --</option>
      </select>
    </div>
    <div id="query-detail-panel" class="q-detail-panel">
      <div class="q-empty">Select a query above to view details</div>
    </div>
  </div>
</div>

<script>
const DATA = {{DATA_JSON}};
const FLAME_GRAPH_DATA = {{FLAMEGRAPH_JSON}};
const COLORS = ['c0','c1','c2','c3','c4'];
const ECHARTS_COLORS = ['#3b82f6','#10b981','#f59e0b','#8b5cf6','#ef4444','#06b6d4','#ec4899'];

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function fmtMs(v) {
  if (v < 1) return v.toFixed(3);
  if (v < 100) return v.toFixed(2);
  return v.toFixed(1);
}

// -- Config Panel --
(function() {
  var panel = document.getElementById('config-panel');
  var modeClass = DATA.mode === 'SERIAL' ? 'mode-serial' : 'mode-concurrent';
  var items = '';

  // Workload
  items += '<div class="config-item"><span class="cfg-label">Workload</span>' +
    '<span class="cfg-value highlight">' + esc(DATA.workload) + '</span></div>';

  // Mode
  items += '<div class="config-item"><span class="cfg-label">Mode</span>' +
    '<span class="cfg-value"><span class="' + modeClass + '">' + DATA.mode + '</span></span></div>';

  if (DATA.mode === 'SERIAL') {
    // Iterations
    items += '<div class="config-item"><span class="cfg-label">Iterations</span>' +
      '<span class="cfg-value">' + DATA.iterations + '</span></div>';
    // Warmup
    items += '<div class="config-item"><span class="cfg-label">Warmup</span>' +
      '<span class="cfg-value">' + DATA.warmup + '</span></div>';
  } else {
    // Concurrency
    items += '<div class="config-item"><span class="cfg-label">Concurrency</span>' +
      '<span class="cfg-value">' + DATA.concurrency + '</span></div>';
    // Duration
    if (DATA.duration > 0) {
      items += '<div class="config-item"><span class="cfg-label">Duration</span>' +
        '<span class="cfg-value">' + DATA.duration + 's</span></div>';
    }
    // Ramp-up
    items += '<div class="config-item"><span class="cfg-label">Ramp-up</span>' +
      '<span class="cfg-value">' + (DATA.ramp_up > 0 ? DATA.ramp_up + 's' : '0') + '</span></div>';
  }

  // Profiling
  if (DATA.has_profile) {
    items += '<div class="config-item"><span class="cfg-label">Profiling</span>' +
      '<span class="cfg-value"><span class="badge-on">🔥 ON</span></span></div>';
  } else {
    items += '<div class="config-item"><span class="cfg-label">Profiling</span>' +
      '<span class="cfg-value"><span class="badge-off">OFF</span></span></div>';
  }

  // Time (same row as above)
  items += '<div class="config-item"><span class="cfg-label">Time</span>' +
    '<span class="cfg-value" style="color:var(--fg2);font-weight:400;font-size:13px">' +
    esc(DATA.timestamp) + '</span></div>';

  // Rows — per-table detail as chips
  var detail = DATA.table_rows_detail || {};
  var tables = Object.keys(detail);
  if (tables.length > 0) {
    var rowsHtml = '<div class="config-item" style="grid-column:1/-1">' +
      '<span class="cfg-label">Table Rows</span><div class="table-rows-wrap">';
    tables.sort();
    tables.forEach(function(t) {
      rowsHtml += '<span class="table-chip">' +
        '<span class="tc-name">' + esc(t) + '</span>' +
        '<span class="tc-sep">·</span>' +
        '<span class="tc-count">' + detail[t].toLocaleString() + '</span></span>';
    });
    rowsHtml += '</div></div>';
    items += rowsHtml;
  } else if (DATA.table_rows > 0) {
    items += '<div class="config-item"><span class="cfg-label">Rows (total)</span>' +
      '<span class="cfg-value">' + DATA.table_rows.toLocaleString() + '</span></div>';
  } else {
    // Fallback: extract table names from setup SQL
    var setupTables = [];
    (DATA.setup_sql || []).forEach(function(sql) {
      var m = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?/i);
      if (m) setupTables.push(m[1]);
    });
    if (setupTables.length > 0) {
      var rowsHtml = '<div class="config-item" style="grid-column:1/-1">' +
        '<span class="cfg-label">Tables</span><div class="table-rows-wrap">';
      setupTables.forEach(function(t) {
        rowsHtml += '<span class="table-chip"><span class="tc-name">' + esc(t) + '</span></span>';
      });
      rowsHtml += '</div></div>';
      items += rowsHtml;
    }
  }

  panel.innerHTML = '<div class="config-grid">' + items + '</div>';
})();

// -- Dashboard cards --
(function() {
  const el = document.getElementById('cards');
  DATA.dbms.forEach((d, i) => {
    el.innerHTML += '<div class="card">' +
      '<div class="label">' + esc(d.name) + '</div>' +
      '<div class="value" style="color:' + ECHARTS_COLORS[i % ECHARTS_COLORS.length] + '">' +
      d.overall_qps + ' <span style="font-size:14px;font-weight:400">QPS</span></div>' +
      '<div class="sub">' + d.total_queries + ' queries in ' + d.total_duration + 's' +
      (d.total_errors > 0 ? ' · <span style="color:var(--red)">' + d.total_errors + ' errors</span>' : '') +
      '</div></div>';
  });
})();

// -- ECharts Grouped Bar Chart --
const METRICS = [
  {key:'avg', label:'Avg Latency (ms)'},
  {key:'p50', label:'P50 Latency (ms)'},
  {key:'p95', label:'P95 Latency (ms)'},
  {key:'p99', label:'P99 Latency (ms)'},
  {key:'qps', label:'QPS (queries/sec)'},
];
let currentMetric = 'avg';
let mainChart = null;

function getAllQueryNames() {
  const names = [];
  DATA.dbms.forEach(d => {
    d.queries.forEach(q => {
      if (names.indexOf(q.name) === -1) names.push(q.name);
    });
  });
  return names;
}

function renderChartMetricTabs() {
  const el = document.getElementById('chart-metric-tabs');
  el.innerHTML = '';
  METRICS.forEach(m => {
    const tab = document.createElement('div');
    tab.className = 'tab' + (m.key === currentMetric ? ' active' : '');
    tab.textContent = m.label.split(' (')[0]; // short label
    tab.onclick = () => { currentMetric = m.key; renderChartMetricTabs(); updateChart(); };
    el.appendChild(tab);
  });
}

function initChart() {
  const dom = document.getElementById('main-chart');
  mainChart = echarts.init(dom, null, {renderer: 'canvas'});
  updateChart();

  window.addEventListener('resize', () => {
    mainChart && mainChart.resize();
  });
}

function updateChart() {
  if (!mainChart) return;
  const queryNames = getAllQueryNames();
  const mInfo = METRICS.find(m => m.key === currentMetric);

  const series = DATA.dbms.map((d, i) => {
    const dataMap = {};
    d.queries.forEach(q => { dataMap[q.name] = q[currentMetric]; });
    return {
      name: d.name,
      type: 'bar',
      barGap: '10%',
      barMaxWidth: 40,
      emphasis: { focus: 'series' },
      itemStyle: { borderRadius: [3, 3, 0, 0] },
      data: queryNames.map(qn => dataMap[qn] || 0),
    };
  });

  const option = {
    color: ECHARTS_COLORS,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      backgroundColor: '#161b22',
      borderColor: '#30363d',
      textStyle: { color: '#c9d1d9', fontSize: 13 },
      formatter: function(params) {
        let html = '<div style="font-weight:700;margin-bottom:6px">' + esc(params[0].axisValue) + '</div>';
        params.forEach(p => {
          const v = currentMetric === 'qps' ? p.value.toFixed(1) : fmtMs(p.value);
          const unit = currentMetric === 'qps' ? ' qps' : ' ms';
          html += '<div style="display:flex;align-items:center;gap:6px;margin:2px 0">' +
            '<span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:' +
            p.color + '"></span>' +
            '<span>' + esc(p.seriesName) + '</span>' +
            '<span style="margin-left:auto;font-weight:600">' + v + unit + '</span></div>';
        });
        return html;
      }
    },
    legend: {
      data: DATA.dbms.map(d => d.name),
      top: 0,
      textStyle: { color: '#8b949e', fontSize: 13 },
      itemWidth: 14, itemHeight: 10, itemGap: 20,
    },
    grid: {
      left: 60, right: 30, top: 50, bottom: queryNames.length > 8 ? 100 : 60,
      containLabel: false,
    },
    xAxis: {
      type: 'category',
      data: queryNames,
      axisLabel: {
        color: '#8b949e',
        fontSize: 12,
        rotate: queryNames.length > 6 ? 35 : 0,
        interval: 0,
      },
      axisLine: { lineStyle: { color: '#30363d' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      name: mInfo.label,
      nameTextStyle: { color: '#8b949e', fontSize: 12, padding: [0, 0, 0, 10] },
      axisLabel: { color: '#8b949e', fontSize: 12 },
      axisLine: { show: false },
      splitLine: { lineStyle: { color: '#21262d', type: 'dashed' } },
    },
    dataZoom: queryNames.length > 12 ? [
      { type: 'slider', bottom: 10, height: 20, borderColor: '#30363d',
        fillerColor: 'rgba(31,111,235,0.2)', handleStyle: { color: '#58a6ff' },
        textStyle: { color: '#8b949e' } },
      { type: 'inside' }
    ] : [],
    series: series,
    animationDuration: 600,
    animationEasing: 'cubicOut',
  };

  mainChart.setOption(option, true);
}

// -- Overall QPS chart (CSS bar) --
function renderQpsChart() {
  const el = document.getElementById('qps-chart');
  if (DATA.dbms.length === 0) { el.innerHTML = ''; return; }
  const maxQps = Math.max(...DATA.dbms.map(d => d.overall_qps), 0.001);
  let html = '';
  DATA.dbms.forEach((d, i) => {
    const pct = (d.overall_qps / maxQps * 100).toFixed(1);
    html += '<div class="chart-row">' +
      '<div class="chart-label">' + esc(d.name) + '</div>' +
      '<div class="chart-bar-bg"><div class="chart-bar ' + COLORS[i % COLORS.length] +
      '" style="width:' + pct + '%">' + d.overall_qps + ' QPS</div></div>' +
      '<div class="chart-val">' + d.total_duration + 's</div></div>';
  });
  el.innerHTML = html;
}

// -- Schema (Setup SQL) rendering --
function highlightSQL(sql) {
  // Tokenise first to protect strings/numbers, then highlight keywords on the rest.
  // This avoids the esc()-then-regex breakage with &#39; entities.
  var tokens = [];
  var re = /('(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)/g;
  var last = 0, m;
  while ((m = re.exec(sql)) !== null) {
    if (m.index > last) tokens.push({type:'code', text: sql.slice(last, m.index)});
    if (m[1] !== undefined) tokens.push({type:'str', text: m[1]});
    else tokens.push({type:'num', text: m[2]});
    last = re.lastIndex;
  }
  if (last < sql.length) tokens.push({type:'code', text: sql.slice(last)});

  var kwRe = /\b(CREATE|TABLE|IF|NOT|EXISTS|INSERT|INTO|SELECT|FROM|WHERE|VALUES|DROP|ALTER|ADD|SET|PRIMARY|KEY|AUTO_INCREMENT|DEFAULT|NULL|UNIQUE|INDEX|CONSTRAINT|FOREIGN|REFERENCES|ON|DELETE|UPDATE|CASCADE|CHECK|ENGINE|CHARSET|COLLATE|COMMENT|LIMIT|AS|AND|OR|IN|ROUND|RAND|CONCAT|SUM|CASE|WHEN|THEN|ELSE|END|VIRTUAL|STORED|GENERATED|ALWAYS|FLOOR|ELT|JSON_ARRAY|JSON_OBJECT|JSON_LENGTH|GLOBAL|ARRAY|CAST|MEMBER|OF|BETWEEN)\b/gi;
  var typeRe = /\b(INT|INTEGER|BIGINT|SMALLINT|TINYINT|MEDIUMINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|VARCHAR|CHAR|TEXT|BLOB|DATE|DATETIME|TIMESTAMP|TIME|YEAR|BOOLEAN|BOOL|ENUM|JSON)\b/gi;

  var out = '';
  tokens.forEach(function(t) {
    if (t.type === 'str') {
      out += '<span class="str">' + esc(t.text) + '</span>';
    } else if (t.type === 'num') {
      out += '<span class="num">' + esc(t.text) + '</span>';
    } else {
      var s = esc(t.text);
      s = s.replace(kwRe, '<span class="kw">$1</span>');
      s = s.replace(typeRe, '<span class="type">$1</span>');
      out += s;
    }
  });
  return out;
}

function formatCreateTable(sql) {
  // Smart formatting: only break at top-level commas and the outer parens
  // of the column definition list, not inside type parens like DECIMAL(10,2)
  // or expressions like CASE WHEN ... END.
  if (sql.indexOf('\n') !== -1) return sql; // already multi-line

  // Find the first '(' that opens the column definition list
  var header = '';
  var body = sql;
  var firstParen = sql.indexOf('(');
  if (firstParen === -1) return sql;
  header = sql.slice(0, firstParen);
  body = sql.slice(firstParen); // starts with '('

  // Walk the body tracking paren depth; break on commas at depth==1
  var depth = 0;
  var lines = [];
  var cur = '';
  for (var i = 0; i < body.length; i++) {
    var ch = body[i];
    if (ch === "'") {
      // skip string literal
      var j = i + 1;
      while (j < body.length && body[j] !== "'") { if (body[j] === '\\') j++; j++; }
      cur += body.slice(i, j + 1);
      i = j;
      continue;
    }
    if (ch === '(') { depth++; cur += ch; continue; }
    if (ch === ')') {
      depth--;
      if (depth === 0) {
        // This is the closing paren of the column list
        if (cur.length > 0) lines.push(cur);
        cur = body.slice(i); // rest including ')' and ENGINE etc.
        break;
      }
      cur += ch;
      continue;
    }
    if (ch === ',' && depth === 1) {
      lines.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }

  if (lines.length <= 1) return sql; // nothing to format

  // Build formatted output
  // First line: header + '(' + first column def
  // Remove leading '(' from first item
  var firstCol = lines[0].replace(/^\(\s*/, '');
  var result = header + '(\n  ' + firstCol.trim();
  for (var k = 1; k < lines.length; k++) {
    result += ',\n  ' + lines[k].trim();
  }
  // Append closing part: ')' + ENGINE/etc suffix
  var suffix = cur.replace(/^\)/, '');
  result += '\n)' + suffix;
  return result;
}

function formatOtherSQL(sql) {
  // Format long INSERT/SET/other SQL for readability
  if (sql.indexOf('\n') !== -1) return sql;
  var s = sql;
  // INSERT INTO ... SELECT: break at major clauses
  if (/^\s*INSERT\s+INTO/i.test(s)) {
    s = s.replace(/\)\s*SELECT\b/gi, ')\nSELECT');
    s = s.replace(/\bSELECT\b/gi, function(m, off) { return off > 0 ? '\nSELECT' : m; });
    s = s.replace(/\bFROM\b/gi, '\nFROM');
    s = s.replace(/\bWHERE\b/gi, '\nWHERE');
    s = s.replace(/\bLIMIT\b/gi, '\nLIMIT');
    // Clean up double newlines
    s = s.replace(/\n\n+/g, '\n');
    s = s.replace(/^\n/, '');
  }
  return s;
}

function renderSchema() {
  var setupSql = DATA.setup_sql || [];
  var teardownSql = DATA.teardown_sql || [];
  var queriesSql = DATA.queries_sql || [];

  // Show section if any data exists
  if (setupSql.length === 0 && teardownSql.length === 0 && queriesSql.length === 0) return;

  var section = document.getElementById('schema-section');
  section.style.display = '';
  var container = document.getElementById('schema-content');

  var html = '';

  // --- Setup SQL ---
  if (setupSql.length > 0) {
    html += '<div class="schema-block">' +
      '<div class="schema-toggle open" id="setup-toggle" onclick="toggleWorkloadSection(\'setup\')">' +
      '<span class="arrow">&#9654;</span> Setup ' +
      '<span class="schema-count">' + setupSql.length + ' statement(s)</span></div>' +
      '<div id="setup-body">';
    setupSql.forEach(function(sql) {
      var formatted = /^\s*CREATE\s+TABLE/i.test(sql) ? formatCreateTable(sql) : formatOtherSQL(sql);
      html += '<div class="schema-sql" style="margin-top:8px">' + highlightSQL(formatted) + '</div>';
    });
    html += '</div></div>';
  }

  // --- Queries ---
  if (queriesSql.length > 0) {
    html += '<div class="schema-block">' +
      '<div class="schema-toggle" id="queries-toggle" onclick="toggleWorkloadSection(\'queries\')">' +
      '<span class="arrow">&#9654;</span> Queries ' +
      '<span class="schema-count">' + queriesSql.length + ' query(s)</span></div>' +
      '<div id="queries-body" style="display:none">';
    queriesSql.forEach(function(q) {
      html += '<div style="margin-top:10px">' +
        '<div style="font-size:12px;color:var(--blue);font-weight:600;margin-bottom:4px">' +
        esc(q.name) +
        '<span style="color:var(--fg2);font-weight:400;margin-left:8px">weight: ' + q.weight + '</span>' +
        (q.description ? '<span style="color:var(--fg2);font-weight:400;margin-left:8px;font-style:italic">' + esc(q.description) + '</span>' : '') +
        '</div>' +
        '<div class="schema-sql">' + highlightSQL(q.sql) + '</div>';
      if (q.cleanup_sql) {
        html += '<div style="margin-top:4px;font-size:11px;color:var(--fg2)">cleanup:</div>' +
          '<div class="schema-sql" style="border-color:var(--yellow);opacity:0.7">' + highlightSQL(q.cleanup_sql) + '</div>';
      }
      html += '</div>';
    });
    html += '</div></div>';
  }

  // --- Teardown SQL ---
  if (teardownSql.length > 0) {
    html += '<div class="schema-block">' +
      '<div class="schema-toggle" id="teardown-toggle" onclick="toggleWorkloadSection(\'teardown\')">' +
      '<span class="arrow">&#9654;</span> Teardown ' +
      '<span class="schema-count">' + teardownSql.length + ' statement(s)</span></div>' +
      '<div id="teardown-body" style="display:none">';
    teardownSql.forEach(function(sql) {
      html += '<div class="schema-sql" style="margin-top:8px">' + highlightSQL(sql) + '</div>';
    });
    html += '</div></div>';
  }

  container.innerHTML = html;
}

function toggleWorkloadSection(name) {
  var toggle = document.getElementById(name + '-toggle');
  var body = document.getElementById(name + '-body');
  if (body.style.display === 'none') {
    body.style.display = '';
    toggle.classList.add('open');
  } else {
    body.style.display = 'none';
    toggle.classList.remove('open');
  }
}

// -- Per-Query Dropdown + Detail Panel --
const DBMS_COLORS_BG = ['#1d4ed8','#047857','#b45309','#6d28d9','#b91c1c','#0e7490','#be185d'];
let currentQueryName = '';

function renderQuerySelector() {
  const select = document.getElementById('query-select');
  const queryNames = getAllQueryNames();

  queryNames.forEach(function(qn) {
    const opt = document.createElement('option');
    opt.value = qn;
    opt.textContent = qn;
    select.appendChild(opt);
  });

  select.addEventListener('change', function() {
    renderQueryDetail(this.value);
  });

  // Auto-select first query
  if (queryNames.length > 0) {
    select.value = queryNames[0];
    renderQueryDetail(queryNames[0]);
  }
}

function renderQueryDetail(qn) {
  const panel = document.getElementById('query-detail-panel');
  currentQueryName = qn;

  if (!qn) {
    panel.innerHTML = '<div class="q-empty">Select a query above to view details</div>';
    return;
  }

  // Collect per-DBMS data for this query
  var perDbms = [];
  var sql = '';
  DATA.dbms.forEach(function(d, di) {
    var q = null;
    d.queries.forEach(function(x) { if (x.name === qn) q = x; });
    if (q) {
      perDbms.push({ dbms: d.name, di: di, q: q });
      if (!sql && q.sql) sql = q.sql;
    }
  });

  // Build flame graph lookup
  var fgByQuery = {};
  FLAME_GRAPH_DATA.forEach(function(fg) {
    fgByQuery[fg.query] = fg.svg || '';
  });

  var html = '';

  // --- Description & SQL ---
  // Look up description and cleanup_sql from queries_sql
  var qMeta = null;
  (DATA.queries_sql || []).forEach(function(qs) { if (qs.name === qn) qMeta = qs; });

  if (qMeta && qMeta.description) {
    html += '<div class="q-sub" style="padding-bottom:8px"><div style="font-size:13px;color:var(--fg2);font-style:italic">' +
      esc(qMeta.description) + '</div></div><hr class="q-separator">';
  }

  if (sql) {
    html += '<div class="q-sub"><div class="q-sub-title"><span class="q-icon">\uD83D\uDCDD</span> SQL</div>' +
      '<div class="sql-code">' + esc(sql) + '</div>';
    if (qMeta && qMeta.cleanup_sql) {
      html += '<div style="margin-top:8px;font-size:12px;color:var(--yellow);font-weight:600">Cleanup SQL</div>' +
        '<div class="sql-code" style="margin-top:4px;border-color:var(--yellow);opacity:0.8">' + esc(qMeta.cleanup_sql) + '</div>';
    }
    html += '</div><hr class="q-separator">';
  }

  // --- Latency Table ---
  html += '<div class="q-sub"><div class="q-sub-title"><span class="q-icon">\u23F1\uFE0F</span> Latency</div>' +
    '<div style="overflow-x:auto"><table>' +
    '<thead><tr><th>DBMS</th>' +
    '<th class="num">Exec</th><th class="num">Errors</th>' +
    '<th class="num">Avg (ms)</th><th class="num">P50 (ms)</th>' +
    '<th class="num">P95 (ms)</th><th class="num">P99 (ms)</th>' +
    '<th class="num">Min (ms)</th><th class="num">Max (ms)</th>' +
    '<th class="num">QPS</th></tr></thead><tbody>';
  perDbms.forEach(function(pd) {
    var q = pd.q;
    var colorBg = DBMS_COLORS_BG[pd.di % DBMS_COLORS_BG.length];
    html += '<tr>' +
      '<td><span class="dbms-tag" style="background:' + colorBg + '">' + esc(pd.dbms) + '</span></td>' +
      '<td class="num">' + q.exec + '</td>' +
      '<td class="num"' + (q.errors > 0 ? ' style="color:var(--red)"' : '') + '>' + q.errors + '</td>' +
      '<td class="num">' + fmtMs(q.avg) + '</td>' +
      '<td class="num">' + fmtMs(q.p50) + '</td>' +
      '<td class="num">' + fmtMs(q.p95) + '</td>' +
      '<td class="num">' + fmtMs(q.p99) + '</td>' +
      '<td class="num">' + fmtMs(q.min) + '</td>' +
      '<td class="num">' + fmtMs(q.max) + '</td>' +
      '<td class="num" style="font-weight:600">' + q.qps.toFixed(1) + '</td></tr>';
  });
  html += '</tbody></table></div></div>';

  // --- EXPLAIN Plans ---
  var hasAnyExplain = false;
  perDbms.forEach(function(pd) { if (pd.q.explain) hasAnyExplain = true; });
  perDbms.forEach(function(pd) { if (pd.q.explain_tree) hasAnyExplain = true; });
  if (hasAnyExplain) {
    html += '<hr class="q-separator">';
    html += '<div class="q-sub"><div class="q-sub-title"><span class="q-icon">\uD83D\uDCCA</span> Execution Plan</div>';
    perDbms.forEach(function(pd) {
      if (pd.q.explain) {
        var colorBg = DBMS_COLORS_BG[pd.di % DBMS_COLORS_BG.length];
        html += '<div class="q-explain-wrap">' +
          '<div class="q-explain-label"><span class="dbms-tag" style="background:' +
          colorBg + ';font-size:11px;padding:1px 6px">' + esc(pd.dbms) + '</span></div>' +
          '<div class="q-explain">' + esc(pd.q.explain) + '</div>';
        if (pd.q.explain_tree) {
          html += '<div class="q-explain-label" style="margin-top:8px">Tree:</div>' +
            '<div class="q-explain q-explain-tree">' + esc(pd.q.explain_tree) + '</div>';
        }
        html += '</div>';
      }
    });
    html += '</div>';
  }

  // --- Flame Graph (tdsql only) ---
  var fgSvg = fgByQuery[qn] || '';
  if (fgSvg) {
    html += '<hr class="q-separator">';
    html += '<div class="q-sub"><div class="q-sub-title"><span class="q-icon">\uD83D\uDD25</span> CPU Flame Graph ' +
      '<span class="fg-badge">tdsql \u00B7 perf</span></div>' +
      '<div class="fg-container" id="fg-detail-' + esc(qn) + '"></div></div>';
  }

  panel.innerHTML = html;

  // Bind flame graph interactivity after DOM update
  if (fgSvg) {
    var fgContainer = document.getElementById('fg-detail-' + esc(qn));
    if (fgContainer) {
      fgContainer.innerHTML = fgSvg;
      fgBindInteractivity(fgContainer);
    }
  }
}

function createSep() {
  var hr = document.createElement('hr');
  hr.className = 'q-separator';
  return hr;
}

/**
 * Bind hover + click-to-zoom interactivity to a flame graph SVG container.
 * This replaces the SVG-embedded <script> which does not execute via innerHTML.
 */
function fgBindInteractivity(container) {
  const svg = container.querySelector('svg');
  if (!svg) return;

  const details = svg.querySelector('.fg-details');
  const frames = svg.querySelectorAll('.fg-frame');
  const chartWidth = parseFloat(svg.getAttribute('data-chart-width') || '1180');
  const xPad = parseFloat(svg.getAttribute('data-x-pad') || '10');
  const fontSize = 12;

  // -- Tooltip element (shared across all flame graph containers) --
  var tooltip = document.getElementById('fg-tooltip');
  var ttName = tooltip ? tooltip.querySelector('.tt-name') : null;
  var ttInfo = tooltip ? tooltip.querySelector('.tt-info') : null;

  // -- Hover → show tooltip near mouse --
  frames.forEach(function(g) {
    g.addEventListener('mouseenter', function() {
      var name = g.getAttribute('data-name') || '';
      var samples = g.getAttribute('data-samples') || '';
      var pct = g.getAttribute('data-pct') || '';
      if (ttName) ttName.textContent = name;
      if (ttInfo) ttInfo.textContent = samples + ' samples (' + pct + '%)';
      if (tooltip) tooltip.style.display = 'block';
      // highlight
      var rect = g.querySelector('rect');
      if (rect) rect.setAttribute('opacity', '0.8');
    });
    g.addEventListener('mousemove', function(e) {
      if (!tooltip) return;
      // Position tooltip near cursor, offset slightly so it doesn't cover the frame
      var tx = e.clientX + 12;
      var ty = e.clientY - 8;
      // Prevent tooltip from going off the right edge
      var tw = tooltip.offsetWidth;
      if (tx + tw > window.innerWidth - 8) {
        tx = e.clientX - tw - 12;
      }
      // Prevent tooltip from going off the bottom edge
      var th = tooltip.offsetHeight;
      if (ty + th > window.innerHeight - 8) {
        ty = e.clientY - th - 8;
      }
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
    });
    g.addEventListener('mouseleave', function() {
      if (tooltip) tooltip.style.display = 'none';
      var rect = g.querySelector('rect');
      if (rect) rect.setAttribute('opacity', '1');
    });
  });

  // -- Click to zoom --
  // We store original positions in data-x, data-y, data-w, data-h.
  // On click, we scale all frames so the clicked frame fills the full width,
  // and hide frames that are not in the clicked subtree.
  var zoomed = false; // track zoom state

  frames.forEach(function(g) {
    g.style.cursor = 'pointer';
    g.addEventListener('click', function(e) {
      e.stopPropagation();
      var clickX = parseFloat(g.getAttribute('data-x'));
      var clickW = parseFloat(g.getAttribute('data-w'));
      var clickY = parseFloat(g.getAttribute('data-y'));

      if (zoomed && clickW > chartWidth * 0.98) {
        // Clicking a full-width frame while zoomed -> reset
        fgResetZoom(svg, frames, chartWidth, xPad, fontSize);
        zoomed = false;
        return;
      }

      // Scale factor: how much to expand clicked frame to fill chart width
      var scale = chartWidth / clickW;
      var offsetX = clickX - xPad;

      zoomed = true;

      frames.forEach(function(f) {
        var fx = parseFloat(f.getAttribute('data-x'));
        var fw = parseFloat(f.getAttribute('data-w'));
        var fy = parseFloat(f.getAttribute('data-y'));
        var fh = parseFloat(f.getAttribute('data-h'));

        // Determine visibility:
        // 1. Frames at the same or deeper level that overlap with clicked range
        // 2. Ancestor frames (at shallower level) that contain the clicked frame
        var fRight = fx + fw;
        var clickRight = clickX + clickW;
        var overlaps = fx < clickRight && fRight > clickX;
        var isAncestor = fy > clickY && fx <= clickX && fRight >= clickRight;
        var isDescendant = fy <= clickY && overlaps;

        if (!overlaps && !isAncestor) {
          // Not in subtree – hide
          f.style.display = 'none';
          return;
        }

        f.style.display = '';
        var rect = f.querySelector('rect');
        var text = f.querySelector('text');
        var clipRect = f.querySelector('clipPath rect');

        // Compute new position: scale and shift
        var newX = xPad + (fx - clickX) * scale;
        var newW = fw * scale;

        // Clamp to chart boundaries
        if (newX < xPad) {
          newW -= (xPad - newX);
          newX = xPad;
        }
        if (newX + newW > xPad + chartWidth) {
          newW = xPad + chartWidth - newX;
        }
        if (newW < 0.1) {
          f.style.display = 'none';
          return;
        }

        if (rect) {
          rect.setAttribute('x', newX.toFixed(1));
          rect.setAttribute('width', newW.toFixed(1));
        }
        // Keep the clipPath rectangle in sync so text is clipped correctly
        if (clipRect) {
          clipRect.setAttribute('x', newX.toFixed(1));
          clipRect.setAttribute('width', newW.toFixed(1));
        }
        if (text) {
          text.setAttribute('x', (newX + 3).toFixed(1));
          // Recompute label truncation with ellipsis
          var name = f.getAttribute('data-name') || '';
          var charW = fontSize * 0.60;
          var availW = newW - 6;
          var maxChars = availW > 0 ? Math.floor(availW / charW) : 0;
          var label;
          if (maxChars >= name.length) {
            label = name;
          } else if (maxChars > 3) {
            label = name.substring(0, maxChars - 1) + '\u2026';
          } else if (maxChars > 0) {
            label = name.substring(0, maxChars);
          } else {
            label = '';
          }
          text.textContent = label;
          text.style.display = newW > 12 ? '' : 'none';
        }
      });

      if (details) details.textContent = 'Zoomed: ' + g.getAttribute('data-name') +
        ' — click full-width frame or background to reset';
    });
  });

  // Click on SVG background to reset zoom
  svg.addEventListener('click', function(e) {
    if (e.target === svg || e.target.tagName === 'rect' && !e.target.closest('.fg-frame')) {
      if (zoomed) {
        fgResetZoom(svg, frames, chartWidth, xPad, fontSize);
        zoomed = false;
      }
    }
  });
}

function fgResetZoom(svg, frames, chartWidth, xPad, fontSize) {
  var details = svg.querySelector('.fg-details');
  frames.forEach(function(f) {
    f.style.display = '';
    var ox = parseFloat(f.getAttribute('data-x'));
    var ow = parseFloat(f.getAttribute('data-w'));
    var rect = f.querySelector('rect');
    var text = f.querySelector('text');
    var clipRect = f.querySelector('clipPath rect');
    if (rect) {
      rect.setAttribute('x', ox.toFixed(1));
      rect.setAttribute('width', ow.toFixed(1));
    }
    if (clipRect) {
      clipRect.setAttribute('x', ox.toFixed(1));
      clipRect.setAttribute('width', ow.toFixed(1));
    }
    if (text) {
      var name = f.getAttribute('data-name') || '';
      var charW = fontSize * 0.60;
      var availW = ow - 6;
      var maxChars = availW > 0 ? Math.floor(availW / charW) : 0;
      var label;
      if (maxChars >= name.length) {
        label = name;
      } else if (maxChars > 3) {
        label = name.substring(0, maxChars - 1) + '\u2026';
      } else if (maxChars > 0) {
        label = name.substring(0, maxChars);
      } else {
        label = '';
      }
      text.textContent = label;
      text.style.display = ow > 12 ? '' : 'none';
      text.setAttribute('x', (ox + 3).toFixed(1));
    }
  });
  if (details) details.textContent = ' ';
}

// -- Init --
renderChartMetricTabs();
initChart();
renderQpsChart();
renderSchema();
renderQuerySelector();
</script>
</body>
</html>"""


def write_bench_html_report(path: str, result: BenchmarkResult):
    """Generate a self-contained HTML benchmark report.

    Uses ECharts (CDN) for an interactive grouped bar chart that puts
    all queries from all DBMS targets side-by-side in one view, plus
    a CSS-based Overall QPS bar, and a detail
    table with DBMS tabs.

    If profiling was enabled, embeds per-query SVG flame graphs.
    """
    data = _build_data(result)

    # Build flame graph data array: [{dbms, query, svg}, ...]
    # Only include tdsql flame graphs — other DBMS profiling is skipped.
    fg_data = []
    seen_svgs = set()  # deduplicate concurrent mode shared SVGs
    for dr in result.dbms_results:
        if dr.dbms_name.lower() != "tdsql":
            continue
        for qs in dr.query_stats:
            if qs.flamegraph_svg:
                # In concurrent mode, all queries share the same SVG;
                # deduplicate by (dbms, svg_hash)
                svg_hash = hash(qs.flamegraph_svg)
                dedup_key = (dr.dbms_name, svg_hash)
                if dedup_key in seen_svgs:
                    continue
                seen_svgs.add(dedup_key)
                fg_data.append({
                    "dbms": dr.dbms_name,
                    "query": qs.query_name,
                    "svg": qs.flamegraph_svg,
                })

    page = _HTML_TEMPLATE
    page = page.replace("{{WORKLOAD}}", _escape(result.workload_name))

    def _safe_json(obj):
        s = json.dumps(obj, ensure_ascii=False)
        s = s.replace("<", "\\u003c")
        return s

    page = page.replace("{{DATA_JSON}}", _safe_json(data))

    # Flame graph data: SVG content is NOT JSON-encoded (it's raw HTML).
    # We build a JS array of objects with the SVG as a string.
    fg_js_items = []
    for fg in fg_data:
        # Escape SVG for safe embedding in JS template literal.
        # CRITICAL: We must escape </script so the browser's HTML parser
        # does not prematurely close the <script> block when it encounters
        # </script> tags embedded inside SVG CDATA sections.
        svg_escaped = (fg["svg"]
                       .replace("\\", "\\\\")
                       .replace("`", "\\`")
                       .replace("${", "\\${")
                       .replace("</script", "<\\/script")
                       .replace("</Script", "<\\/Script")
                       .replace("</SCRIPT", "<\\/SCRIPT"))
        fg_js_items.append(
            '{dbms:' + json.dumps(fg["dbms"]) +
            ',query:' + json.dumps(fg["query"]) +
            ',svg:`' + svg_escaped + '`}'
        )
    fg_js = "[" + ",".join(fg_js_items) + "]" if fg_js_items else "[]"
    page = page.replace("{{FLAMEGRAPH_JSON}}", fg_js)

    with open(path, "w", encoding="utf-8") as f:
        f.write(page)

    log.info("Benchmark HTML report written: %s", path)
