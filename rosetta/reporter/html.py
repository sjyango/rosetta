"""HTML report generator for Rosetta.

Generates a single self-contained HTML file with:
- Dashboard: summary table, pass-rate bars
- Diff details: side-by-side view with syntax highlighting
"""

import html
import json
import logging
import time
from typing import Dict, List, Optional

from ..models import CompareResult, Statement, StmtType

log = logging.getLogger("rosetta")


def _escape(text: str) -> str:
    """HTML-escape a string."""
    return html.escape(text, quote=True)


def _build_summary_data(comparisons: Dict[str, CompareResult]) -> List[dict]:
    """Build summary data for the template."""
    rows = []
    for key, cmp in comparisons.items():
        rows.append({
            "key": key,
            "dbms_a": cmp.dbms_a,
            "dbms_b": cmp.dbms_b,
            "matched": cmp.matched,
            "mismatched": cmp.mismatched,
            "effective_mismatched": cmp.effective_mismatched,
            "skipped": cmp.skipped,
            "total": cmp.total_stmts,
            "pass_rate": round(cmp.pass_rate, 1),
        })
    return rows


def _build_diff_data(comparisons: Dict[str, CompareResult]) -> List[dict]:
    """Build diff data for the template (includes all diffs, front-end filters)."""
    sections = []
    for key, cmp in comparisons.items():
        if not cmp.diffs:
            continue
        diffs = []
        for d in cmp.diffs:
            diffs.append({
                "block": d["block"],
                "stmt": d["stmt"][:200],
                "lines_a": d.get("lines_a", []),
                "lines_b": d.get("lines_b", []),
                "context_before": d.get("context_before", []),
                "context_after": d.get("context_after", []),
                "skipped": d.get("skipped", False),
                "skip_reason": d.get("skip_reason", ""),
            })
        sections.append({
            "key": key,
            "dbms_a": cmp.dbms_a,
            "dbms_b": cmp.dbms_b,
            "diffs": diffs,
        })
    return sections


def _build_sql_list_data(sql_list: Optional[List[Statement]]) -> List[dict]:
    """Build SQL list data for the template."""
    if not sql_list:
        return []
    return [
        {
            "idx": i + 1,
            "sql": s.text,
            "skipped": s.stmt_type == StmtType.SKIP,
        }
        for i, s in enumerate(sql_list)
        if s.text.strip()
    ]


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta Report — {{TEST_NAME}}</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --orange: #db8b0b; --orange-bg: #2d2009;
  --purple: #a371f7; --purple-bg: #1e163b;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { margin-bottom: 4px; font-size: 24px; display: flex; align-items: center; gap: 8px; }
h1 .brand { background: linear-gradient(135deg, #1f6feb 0%, #58a6ff 50%, #79c0ff 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text; font-weight: 700; }
h1 .title-rest { background: linear-gradient(135deg, #1f6feb 0%, #58a6ff 50%, #79c0ff 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; font-weight: 600; }
.btn-nav { color: var(--fg2); font-size: 13px; border: 1px solid var(--border);
  border-radius: 6px; padding: 5px 14px; background: var(--bg3); text-decoration: none;
  transition: all 0.15s; display: inline-block; }
.btn-nav:hover { border-color: var(--blue); color: var(--blue); background: var(--bg2); }
.meta { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }
.meta span { margin-right: 16px; }

/* Summary table */
.summary-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 20px; margin-bottom: 24px; }
.summary-card h2 { font-size: 18px; margin-bottom: 12px; }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th { text-align: left; padding: 8px 12px; border-bottom: 2px solid var(--border);
  color: var(--fg2); font-weight: 600; }
td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
tr:hover { background: var(--bg3); }
.pass-bar { display: inline-block; height: 8px; border-radius: 4px;
  background: var(--green); vertical-align: middle; }
.fail-bar { display: inline-block; height: 8px; border-radius: 4px;
  background: var(--red); vertical-align: middle; }
.bar-bg { display: inline-block; width: 120px; height: 8px; border-radius: 4px;
  background: var(--bg3); vertical-align: middle; position: relative; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 4px; position: absolute; left: 0; top: 0; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px;
  font-size: 12px; font-weight: 600; }
.badge-pass { background: var(--green-bg); color: var(--green); }
.badge-fail { background: var(--red-bg); color: var(--red); }
.num-mismatch { color: var(--red); font-weight: 600; }
.num-match { color: var(--green); }
.num-wl { color: var(--orange); }
.num-sql-wl { color: #e6a817; }
.num-bug { color: var(--red); }

/* Filter bar */
.filter-bar { display: flex; gap: 8px; align-items: center;
  margin-bottom: 16px; flex-wrap: wrap; padding: 10px 16px;
  background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; }
.filter-bar input { background: var(--bg); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 12px; color: var(--fg); font-size: 13px;
  width: 260px; outline: none; transition: border-color 0.15s; }
.filter-bar input:focus { border-color: var(--accent); }
.filter-btn { display: inline-flex; align-items: center; gap: 4px;
  background: none; border: 1px solid transparent; border-radius: 6px;
  padding: 4px 10px; cursor: pointer; font-size: 13px; font-weight: 600;
  transition: all 0.15s; color: var(--fg2); }
.filter-btn:hover { background: var(--bg3); border-color: var(--border); }
.filter-btn.active { background: var(--bg3); border-color: var(--accent);
  box-shadow: 0 0 0 1px var(--accent); color: var(--fg); }
.filter-btn-all { color: var(--fg2); }
.filter-btn-all.active { background: var(--bg3); border-color: var(--fg2); box-shadow: none; }
.filter-btn-diff { color: var(--red); }
.filter-btn-diff.active { background: var(--red-bg); border-color: var(--red); box-shadow: none; }
.filter-btn-skip { color: var(--fg2); }
.filter-btn-skip.active { background: var(--bg3); border-color: var(--fg2); box-shadow: none; }
.filter-sep { width: 1px; height: 20px; background: var(--border); margin: 0 4px; }

/* Diff sections */
.diff-section { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; margin-bottom: 16px; overflow: hidden; }
.diff-section.skipped-block { opacity: 0.5; border-left: none; }
.diff-header { padding: 12px 16px; cursor: pointer; display: flex;
  align-items: center; gap: 12px; user-select: none; }
.diff-header:hover { background: var(--bg3); }
.diff-header .arrow { transition: transform 0.2s; color: var(--fg2); }
.diff-header.open .arrow { transform: rotate(90deg); }
.diff-header .block-num { color: var(--fg2); font-size: 13px; min-width: 80px; }
.diff-header .sql-preview { font-family: 'SF Mono', Consolas, monospace;
  font-size: 13px; color: var(--blue); overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; flex: 1; }
.diff-body { display: block; border-top: 1px solid var(--border); }
.diff-body.collapsed { display: none; }

/* Side-by-side diff */
.side-by-side { display: grid; grid-template-columns: 1fr 1fr; }
.diff-pane { overflow-x: auto; }
.diff-pane-header { padding: 8px 12px; font-size: 13px; font-weight: 600;
  color: var(--fg2); background: var(--bg3); border-bottom: 1px solid var(--border); }
.diff-pane:first-child { border-right: 1px solid var(--border); }
.diff-line { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  padding: 1px 12px; white-space: pre-wrap; word-break: break-all;
  min-height: 22px; line-height: 22px; }
.diff-line.added { background: var(--green-bg); }
.diff-line.removed { background: var(--red-bg); }
.diff-line.context { }
.diff-line.empty { color: var(--bg3); }

/* Comparison tab */
.comp-tabs { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
.comp-tab { padding: 6px 16px; border-radius: 6px; cursor: pointer;
  font-size: 14px; border: 1px solid var(--border); background: var(--bg2);
  color: var(--fg2); }
.comp-tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }
.comp-tab .tab-count { font-size: 12px; margin-left: 4px; }

.no-diff { padding: 40px; text-align: center; color: var(--fg2); font-size: 16px; }

/* Context lines (surrounding blocks) */
.context-bar { padding: 6px 16px; font-size: 12px; color: var(--fg2);
  background: var(--bg); border-bottom: 1px solid var(--border);
  font-family: 'SF Mono', Consolas, monospace; line-height: 1.6; }
.context-bar .ctx-label { color: var(--fg2); font-weight: 600;
  margin-right: 6px; font-size: 11px; text-transform: uppercase; }
.context-bar .ctx-item { display: block; padding: 1px 0; }
.context-bar .ctx-block { color: var(--yellow); margin-right: 4px; }
.context-bar .ctx-sql { color: var(--fg2); }
.context-bar .ctx-current { color: var(--red); font-weight: 600; }

/* Toast notification */
.toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
  border-radius: 8px; font-size: 14px; color: #fff; z-index: 9999;
  transition: opacity 0.3s; pointer-events: none; }
.toast-success { background: var(--green); }
.toast-error { background: var(--red); }

/* Responsive */
@media (max-width: 900px) {
  .side-by-side { grid-template-columns: 1fr; }
  .diff-pane:first-child { border-right: none; border-bottom: 1px solid var(--border); }
}

/* SQL list */
.sql-toggle { display: flex; align-items: center; gap: 10px;
  margin-bottom: 16px; cursor: pointer; user-select: none;
  padding: 10px 16px; background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; }
.sql-toggle:hover { background: var(--bg3); }
.sql-toggle .arrow { transition: transform 0.2s; color: var(--fg2); }
.sql-toggle.open .arrow { transform: rotate(90deg); }
.sql-toggle .label { font-size: 14px; font-weight: 600; color: var(--fg); }
.sql-toggle .count { font-size: 13px; color: var(--fg2); }
.sql-list { display: none; margin-bottom: 24px; }
.sql-list.open { display: block; }
.sql-list-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; overflow: hidden; }
.sql-item { display: flex; align-items: flex-start; padding: 4px 16px;
  border-bottom: 1px solid var(--border); font-size: 13px; }
.sql-item:last-child { border-bottom: none; }
.sql-item:hover { background: var(--bg3); }
.sql-idx { color: var(--fg2); min-width: 36px; padding-top: 2px;
  font-family: 'SF Mono', Consolas, monospace; font-size: 12px; }
.sql-text { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); white-space: pre-wrap; word-break: break-all;
  line-height: 1.5; }
.sql-item.skipped { opacity: 0.5; }
.sql-item.skipped .sql-text { color: var(--fg2); text-decoration: line-through; }
.sql-item.skipped .sql-idx::after { content: '⊘'; margin-left: 2px;
  color: var(--yellow); font-size: 11px; }
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1><svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" stroke="#58a6ff" stroke-width="2" fill="none"/><line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/><line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="20" x2="23" y2="20" stroke="#d29922" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/></svg> <span class="brand">Rosetta</span> <span class="title-rest">Report</span></h1>
    <a href="../index.html" class="btn-nav">&#9664; History</a>
    <a href="../playground.html" class="btn-nav">&#9654; Playground</a>
  </div>
  <div class="meta">
    <span>Test: <strong>{{TEST_NAME}}</strong></span>
    <span>Time: {{TIME}}</span>
    <span>Baseline: <strong>{{BASELINE}}</strong></span>
  </div>

  <div class="sql-list" id="sql-list-section">
    <div class="sql-list-card" id="sql-list-body"></div>
  </div>
  <div class="sql-toggle" id="sql-list-toggle" style="display:none">
    <span class="arrow">&#9654;</span>
    <span class="label">Executed SQL</span>
    <span class="count" id="sql-list-count"></span>
  </div>

  <div class="summary-card">
    <h2>Summary</h2>
    <table>
      <thead>
        <tr>
          <th>Comparison</th><th>Status</th><th>Match</th>
          <th>Mismatch</th><th>Total</th><th>Pass Rate</th>
        </tr>
      </thead>
      <tbody id="summary-body"></tbody>
    </table>
  </div>

  <div id="diff-container">
    <div class="comp-tabs" id="comp-tabs"></div>
    <div class="filter-bar" id="filter-bar">
      <button class="filter-btn filter-btn-all active" data-filter="all">All</button>
      <button class="filter-btn filter-btn-diff" data-filter="active">Diff</button>
      <button class="filter-btn filter-btn-skip" data-filter="skipped">Skip</button>
      <span class="filter-sep"></span>
      <input type="text" id="search-input" placeholder="Search SQL...">
    </div>
    <div id="diff-list"></div>
  </div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
const SUMMARY = {{SUMMARY_JSON}};
const DIFFS = {{DIFFS_JSON}};
const SQL_LIST = {{SQL_LIST_JSON}};

// Render SQL list
(function() {
  if (!SQL_LIST || SQL_LIST.length === 0) return;
  const toggle = document.getElementById('sql-list-toggle');
  const section = document.getElementById('sql-list-section');
  const body = document.getElementById('sql-list-body');
  const countEl = document.getElementById('sql-list-count');
  toggle.style.display = 'flex';
  countEl.textContent = '(' + SQL_LIST.length + ' statements)';
  const frag = document.createDocumentFragment();
  SQL_LIST.forEach(s => {
    const item = document.createElement('div');
    item.className = 'sql-item' + (s.skipped ? ' skipped' : '');
    item.innerHTML = '<span class="sql-idx">' + s.idx + '</span><span class="sql-text">' + esc(s.sql) + '</span>';
    frag.appendChild(item);
  });
  body.appendChild(frag);
  toggle.onclick = () => {
    toggle.classList.toggle('open');
    section.classList.toggle('open');
  };
})();

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast toast-' + (type || 'success');
  t.style.opacity = '1';
  setTimeout(() => { t.style.opacity = '0'; }, 2500);
}

// Render summary table
const tbody = document.getElementById('summary-body');
SUMMARY.forEach(r => {
  const effectiveMismatch = r.effective_mismatched || 0;
  const status = effectiveMismatch <= 0;
  const pct = r.pass_rate;
  const row = document.createElement('tr');
  row.innerHTML = `
    <td>${esc(r.key)}</td>
    <td><span class="badge ${status ? 'badge-pass' : 'badge-fail'}">${status ? 'PASS' : 'FAIL'}</span></td>
    <td class="num-match">${r.matched}</td>
    <td class="${effectiveMismatch > 0 ? 'num-mismatch' : ''}">${effectiveMismatch > 0 ? effectiveMismatch : 0}</td>
    <td>${r.total}</td>
    <td>
      <span class="bar-bg"><span class="bar-fill" style="width:${pct}%;background:${pct>=100?'var(--green)':pct>=90?'var(--yellow)':'var(--red)'}"></span></span>
      ${pct}%
    </td>
  `;
  tbody.appendChild(row);
});

// Render comparison tabs
const tabsEl = document.getElementById('comp-tabs');
const listEl = document.getElementById('diff-list');
let activeTab = DIFFS.length > 0 ? DIFFS[0].key : null;

function renderTabs() {
  tabsEl.innerHTML = '';
  if (DIFFS.length === 0) {
    listEl.innerHTML = '<div class="no-diff">All results matched — no differences found.</div>';
    return;
  }
  DIFFS.forEach(sec => {
    const active = sec.diffs.filter(d => !d.skipped).length;
    const skips = sec.diffs.filter(d => d.skipped).length;
    const tab = document.createElement('div');
    tab.className = 'comp-tab' + (sec.key === activeTab ? ' active' : '');
    let label = `${esc(sec.key)}<span class="tab-count">(${active}`;
    if (skips > 0) label += ` +${skips} skip`;
    label += ')</span>';
    tab.innerHTML = label;
    tab.onclick = () => { activeTab = sec.key; renderTabs(); renderDiffs(); };
    tabsEl.appendChild(tab);
  });
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/`/g,'&#96;').replace(/\$\{/g,'&#36;{');
}

function buildDiffBody(d, sec) {
  const normA = d.lines_a.map(l => l.trim());
  const normB = d.lines_b.map(l => l.trim());
  const matchedA = new Set();
  const matchedB = new Set();
  let bStart = 0;
  for (let ai = 0; ai < normA.length; ai++) {
    for (let bi = bStart; bi < normB.length; bi++) {
      if (normA[ai] === normB[bi] && !matchedB.has(bi)) {
        matchedA.add(ai); matchedB.add(bi); bStart = bi + 1; break;
      }
    }
  }
  const left = []; const right = [];
  d.lines_a.forEach((l, i) => {
    left.push(`<div class="diff-line ${matchedA.has(i)?'context':'removed'}">${esc(l)}</div>`);
  });
  d.lines_b.forEach((l, i) => {
    right.push(`<div class="diff-line ${matchedB.has(i)?'context':'added'}">${esc(l)}</div>`);
  });
  return `<div class="side-by-side"><div class="diff-pane"><div class="diff-pane-header">${esc(sec.dbms_a)} (baseline)</div>${left.join('')}</div><div class="diff-pane"><div class="diff-pane-header">${esc(sec.dbms_b)}</div>${right.join('')}</div></div>`;
}

function renderDiffs() {
  listEl.innerHTML = '';
  const sec = DIFFS.find(s => s.key === activeTab);
  if (!sec) return;
  const query = document.getElementById('search-input').value.toLowerCase();
  const activeBtn = document.querySelector('#filter-bar .filter-btn.active');
  const wlFilter = activeBtn ? activeBtn.getAttribute('data-filter') : 'all';

  const frag = document.createDocumentFragment();
  sec.diffs.forEach(d => {
    if (query && !d.stmt.toLowerCase().includes(query)) return;
    if (wlFilter === 'active' && d.skipped) return;
    if (wlFilter === 'skipped' && !d.skipped) return;

    const section = document.createElement('div');
    let sectionCls = 'diff-section';
    if (d.skipped) sectionCls += ' skipped-block';
    section.className = sectionCls;

    const header = document.createElement('div');
    header.className = 'diff-header' + (d.skipped ? '' : ' open');
    const skipTag = d.skipped ? '<span class="badge" style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;background:#888;color:#fff;white-space:nowrap">skipped</span>' : '';
    header.innerHTML = `<span class="arrow">&#9654;</span><span class="block-num">Block ${d.block}</span>${skipTag}<span class="sql-preview">${esc(d.stmt)}</span>`;

    const body = document.createElement('div');
    body.className = 'diff-body' + (d.skipped ? ' collapsed' : '');

    if (d.skipped) {
      // Skipped block: show reason and single-side output
      const skipInfo = document.createElement('div');
      skipInfo.style.cssText = 'padding: 8px 12px; color: #888; font-style: italic;';
      const reason = d.skip_reason || 'Skipped (only exists on one side)';
      skipInfo.textContent = '\u26A0 ' + reason;
      body.appendChild(skipInfo);
      // Show output from whichever side has it
      if (d.lines_a && d.lines_a.length > 0) {
        const pre = document.createElement('pre');
        pre.className = 'diff-content';
        pre.textContent = d.lines_a.join('\\n');
        body.appendChild(pre);
      }
      if (d.lines_b && d.lines_b.length > 0) {
        const pre = document.createElement('pre');
        pre.className = 'diff-content';
        pre.textContent = d.lines_b.join('\\n');
        body.appendChild(pre);
      }
      section.appendChild(header);
      section.appendChild(body);
      frag.appendChild(section);
      return;
    }
    // Render context bar
    const ctxBefore = d.context_before || [];
    const ctxAfter = d.context_after || [];
    if (ctxBefore.length > 0 || ctxAfter.length > 0) {
      const ctxBar = document.createElement('div');
      ctxBar.className = 'context-bar';
      let ctxHtml = '';
      ctxBefore.forEach(c => {
        ctxHtml += `<span class="ctx-item"><span class="ctx-block">Block ${c.block}</span><span class="ctx-sql">${esc(c.stmt)}</span></span>`;
      });
      ctxHtml += `<span class="ctx-item"><span class="ctx-block">Block ${d.block}</span><span class="ctx-current">&#9654; ${esc(d.stmt)}</span></span>`;
      ctxAfter.forEach(c => {
        ctxHtml += `<span class="ctx-item"><span class="ctx-block">Block ${c.block}</span><span class="ctx-sql">${esc(c.stmt)}</span></span>`;
      });
      ctxBar.innerHTML = ctxHtml;
      body.appendChild(ctxBar);
    }

    const diffContent = document.createElement('div');
    diffContent.innerHTML = buildDiffBody(d, sec);
    body.appendChild(diffContent);

    header.onclick = () => {
      header.classList.toggle('open');
      body.classList.toggle('collapsed');
    };

    section.appendChild(header);
    section.appendChild(body);
    frag.appendChild(section);
  });
  listEl.appendChild(frag);

  if (listEl.children.length === 0) {
    listEl.innerHTML = '<div class="no-diff">No differences match the current filter.</div>';
  }
}

document.getElementById('search-input').addEventListener('input', renderDiffs);
document.querySelectorAll('#filter-bar .filter-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#filter-bar .filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    renderDiffs();
  });
});

renderTabs();
renderDiffs();
</script>
</body>
</html>"""


def write_html_report(path: str, test_file: str,
                      comparisons: Dict[str, CompareResult],
                      baseline: str = "",
                      sql_list: Optional[List[Statement]] = None):
    """Generate a self-contained HTML report file."""
    summary = _build_summary_data(comparisons)
    diffs = _build_diff_data(comparisons)
    sql_data = _build_sql_list_data(sql_list)

    test_name = test_file.rsplit("/", 1)[-1] if "/" in test_file else test_file

    page = _HTML_TEMPLATE
    page = page.replace("{{TEST_NAME}}", _escape(test_name))
    page = page.replace("{{TIME}}",
                         _escape(time.strftime("%Y-%m-%d %H:%M:%S")))
    page = page.replace("{{BASELINE}}", _escape(baseline or "N/A"))
    # Safely embed JSON in <script>: escape '</' to prevent breaking
    # the script tag, and escape backslash sequences that might confuse
    # the JS parser.
    def _safe_json(obj):
        s = json.dumps(obj, ensure_ascii=False)
        # Prevent "</script>" or any "</" from closing the script element
        s = s.replace("<", "\\u003c")
        return s

    page = page.replace("{{SUMMARY_JSON}}", _safe_json(summary))
    page = page.replace("{{DIFFS_JSON}}", _safe_json(diffs))
    page = page.replace("{{SQL_LIST_JSON}}", _safe_json(sql_data))

    with open(path, "w", encoding="utf-8") as f:
        f.write(page)

    log.info("HTML report written: %s", path)
