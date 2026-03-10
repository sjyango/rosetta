"""HTML report generator for Rosetta.

Generates a single self-contained HTML file with:
- Dashboard: summary table, pass-rate bars
- Diff details: side-by-side view with syntax highlighting
"""

import html
import json
import logging
import time
from typing import Dict, List

from ..models import CompareResult

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
            "skipped": cmp.skipped,
            "total": cmp.total_stmts,
            "pass_rate": round(cmp.pass_rate, 1),
        })
    return rows


def _build_diff_data(comparisons: Dict[str, CompareResult]) -> List[dict]:
    """Build diff data for the template."""
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
            })
        sections.append({
            "key": key,
            "dbms_a": cmp.dbms_a,
            "dbms_b": cmp.dbms_b,
            "diffs": diffs,
        })
    return sections


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
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1400px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 24px; }
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

/* Filter bar */
.filter-bar { display: flex; gap: 12px; align-items: center;
  margin-bottom: 16px; flex-wrap: wrap; }
.filter-bar input { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 12px; color: var(--fg); font-size: 14px;
  width: 300px; outline: none; }
.filter-bar input:focus { border-color: var(--accent); }
.filter-bar select { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 12px; color: var(--fg); font-size: 14px;
  outline: none; }

/* Diff sections */
.diff-section { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; margin-bottom: 16px; overflow: hidden; }
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

/* Responsive */
@media (max-width: 900px) {
  .side-by-side { grid-template-columns: 1fr; }
  .diff-pane:first-child { border-right: none; border-bottom: 1px solid var(--border); }
}
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1>Rosetta Report</h1>
    <a href="../index.html" style="color:var(--blue);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9664; History</a>
  </div>
  <div class="meta">
    <span>Test: <strong>{{TEST_NAME}}</strong></span>
    <span>Time: {{TIME}}</span>
    <span>Baseline: <strong>{{BASELINE}}</strong></span>
  </div>

  <div class="summary-card">
    <h2>Summary</h2>
    <table>
      <thead>
        <tr>
          <th>Comparison</th><th>Status</th><th>Match</th><th>Mismatch</th>
          <th>Skip</th><th>Total</th><th>Pass Rate</th>
        </tr>
      </thead>
      <tbody id="summary-body"></tbody>
    </table>
  </div>

  <div id="diff-container">
    <div class="comp-tabs" id="comp-tabs"></div>
    <div class="filter-bar">
      <input type="text" id="search-input" placeholder="Search SQL statements...">
    </div>
    <div id="diff-list"></div>
  </div>
</div>

<script>
const SUMMARY = {{SUMMARY_JSON}};
const DIFFS = {{DIFFS_JSON}};

// Render summary table
const tbody = document.getElementById('summary-body');
SUMMARY.forEach(r => {
  const status = r.mismatched === 0;
  const pct = r.pass_rate;
  const row = document.createElement('tr');
  row.innerHTML = `
    <td>${esc(r.key)}</td>
    <td><span class="badge ${status ? 'badge-pass' : 'badge-fail'}">${status ? 'PASS' : 'FAIL'}</span></td>
    <td class="num-match">${r.matched}</td>
    <td class="${r.mismatched > 0 ? 'num-mismatch' : ''}">${r.mismatched}</td>
    <td>${r.skipped}</td>
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
    const tab = document.createElement('div');
    tab.className = 'comp-tab' + (sec.key === activeTab ? ' active' : '');
    tab.innerHTML = `${esc(sec.key)}<span class="tab-count">(${sec.diffs.length})</span>`;
    tab.onclick = () => { activeTab = sec.key; renderTabs(); renderDiffs(); };
    tabsEl.appendChild(tab);
  });
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
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

  const frag = document.createDocumentFragment();
  sec.diffs.forEach(d => {
    if (query && !d.stmt.toLowerCase().includes(query)) return;

    const section = document.createElement('div');
    section.className = 'diff-section';

    const header = document.createElement('div');
    header.className = 'diff-header open';
    header.innerHTML = `<span class="arrow">&#9654;</span><span class="block-num">Block ${d.block}</span><span class="sql-preview">${esc(d.stmt)}</span>`;

    const body = document.createElement('div');
    body.className = 'diff-body';

    // Render context bar (surrounding blocks for quick orientation)
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
renderTabs();
renderDiffs();
</script>
</body>
</html>"""


def write_html_report(path: str, test_file: str,
                      comparisons: Dict[str, CompareResult],
                      baseline: str = ""):
    """Generate a self-contained HTML report file."""
    summary = _build_summary_data(comparisons)
    diffs = _build_diff_data(comparisons)

    test_name = test_file.rsplit("/", 1)[-1] if "/" in test_file else test_file

    page = _HTML_TEMPLATE
    page = page.replace("{{TEST_NAME}}", _escape(test_name))
    page = page.replace("{{TIME}}",
                         _escape(time.strftime("%Y-%m-%d %H:%M:%S")))
    page = page.replace("{{BASELINE}}", _escape(baseline or "N/A"))
    page = page.replace("{{SUMMARY_JSON}}",
                         json.dumps(summary, ensure_ascii=False))
    page = page.replace("{{DIFFS_JSON}}",
                         json.dumps(diffs, ensure_ascii=False))

    with open(path, "w", encoding="utf-8") as f:
        f.write(page)

    log.info("HTML report written: %s", path)
