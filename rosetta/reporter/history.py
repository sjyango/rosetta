"""History index page generator for Rosetta.

Scans the output directory for timestamped run sub-directories and generates
a single ``index.html`` that lists all historical runs with links to their
HTML reports.
"""

import html
import json
import logging
import os
import re
from typing import List

log = logging.getLogger("rosetta")

# Pattern: <test_name>_YYYYMMDD_HHMMSS
_RUN_DIR_RE = re.compile(r"^(.+)_(\d{8}_\d{6})$")


def _scan_runs(output_dir: str) -> List[dict]:
    """Return a list of run metadata dicts, newest first."""
    runs = []
    for entry in os.listdir(output_dir):
        full = os.path.join(output_dir, entry)
        if not os.path.isdir(full):
            continue
        m = _RUN_DIR_RE.match(entry)
        if not m:
            continue

        test_name = m.group(1)
        stamp = m.group(2)  # YYYYMMDD_HHMMSS

        # Look for HTML report
        html_file = f"{test_name}.html"
        html_path = os.path.join(full, html_file)
        has_html = os.path.isfile(html_path)

        # Look for text report to extract summary
        report_file = f"{test_name}.report.txt"
        report_path = os.path.join(full, report_file)
        summary_line = ""
        if os.path.isfile(report_path):
            with open(report_path, "r", encoding="utf-8") as f:
                for line in f:
                    if "Pass%" in line or "SUMMARY" in line:
                        continue
                    if "%" in line and ("vs" in line or "_vs_" in line):
                        summary_line = line.strip()
                        break

        # Format timestamp for display
        display_time = (f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]} "
                        f"{stamp[9:11]}:{stamp[11:13]}:{stamp[13:15]}")

        # Count result files
        result_files = [f for f in os.listdir(full) if f.endswith(".result")]
        dbms_names = sorted(set(
            f.rsplit(".", 2)[1] for f in result_files
            if f.count(".") >= 2
        ))

        runs.append({
            "dir_name": entry,
            "test_name": test_name,
            "stamp": stamp,
            "display_time": display_time,
            "has_html": has_html,
            "html_link": f"{entry}/{html_file}" if has_html else "",
            "report_link": f"{entry}/{report_file}" if os.path.isfile(report_path) else "",
            "dbms": dbms_names,
            "summary": summary_line,
        })

    runs.sort(key=lambda r: r["stamp"], reverse=True)
    return runs


_INDEX_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta — History</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --red: #f85149;
  --blue: #58a6ff; --yellow: #d29922;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 28px; }
.subtitle { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }

/* Filter */
.toolbar { display: flex; gap: 12px; margin-bottom: 20px; align-items: center; flex-wrap: wrap; }
.toolbar input, .toolbar select {
  background: var(--bg2); border: 1px solid var(--border); border-radius: 6px;
  padding: 8px 14px; color: var(--fg); font-size: 14px; outline: none; }
.toolbar input:focus, .toolbar select:focus { border-color: var(--accent); }
.toolbar input { width: 300px; }
.count-label { color: var(--fg2); font-size: 14px; margin-left: auto; }

/* Cards */
.run-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px 20px; margin-bottom: 12px;
  display: flex; align-items: center; gap: 20px; transition: border-color 0.15s; }
.run-card:hover { border-color: var(--accent); }
.run-time { color: var(--fg2); font-size: 13px; min-width: 160px; font-family: 'SF Mono', Consolas, monospace; }
.run-test { font-weight: 600; font-size: 15px; min-width: 280px; }
.run-dbms { display: flex; gap: 6px; flex-wrap: wrap; flex: 1; }
.dbms-tag { background: var(--bg3); border: 1px solid var(--border);
  border-radius: 4px; padding: 2px 8px; font-size: 12px; color: var(--fg2); }
.run-actions { display: flex; gap: 8px; }
.btn { display: inline-block; padding: 6px 14px; border-radius: 6px;
  font-size: 13px; font-weight: 500; text-decoration: none; cursor: pointer; border: none; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-primary:hover { opacity: 0.9; }
.btn-secondary { background: var(--bg3); color: var(--fg2); border: 1px solid var(--border); }
.btn-secondary:hover { color: var(--fg); border-color: var(--fg2); }
.empty-state { text-align: center; padding: 60px 20px; color: var(--fg2); font-size: 16px; }
</style>
</head>
<body>
<div class="container">
  <h1>Rosetta History</h1>
  <div class="subtitle">Cross-DBMS SQL behavioral consistency verification</div>

  <div class="toolbar">
    <input type="text" id="filter-input" placeholder="Filter by test name...">
    <select id="filter-dbms"><option value="">All DBMS</option></select>
    <span class="count-label" id="count-label"></span>
  </div>

  <div id="run-list"></div>
</div>

<script>
const RUNS = {{RUNS_JSON}};

// Populate DBMS filter
const allDbms = [...new Set(RUNS.flatMap(r => r.dbms))].sort();
const dbmsSelect = document.getElementById('filter-dbms');
allDbms.forEach(d => {
  const opt = document.createElement('option');
  opt.value = d; opt.textContent = d;
  dbmsSelect.appendChild(opt);
});

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function render() {
  const nameFilter = document.getElementById('filter-input').value.toLowerCase();
  const dbmsFilter = document.getElementById('filter-dbms').value;
  const listEl = document.getElementById('run-list');
  listEl.innerHTML = '';

  let count = 0;
  RUNS.forEach(r => {
    if (nameFilter && !r.test_name.toLowerCase().includes(nameFilter)) return;
    if (dbmsFilter && !r.dbms.includes(dbmsFilter)) return;
    count++;

    const card = document.createElement('div');
    card.className = 'run-card';

    const dbmsTags = r.dbms.map(d => `<span class="dbms-tag">${esc(d)}</span>`).join('');

    let actions = '';
    if (r.has_html) {
      actions += `<a class="btn btn-primary" href="${esc(r.html_link)}">View Report</a>`;
    }
    if (r.report_link) {
      actions += `<a class="btn btn-secondary" href="${esc(r.report_link)}">Text Report</a>`;
    }

    card.innerHTML = `
      <div class="run-time">${esc(r.display_time)}</div>
      <div class="run-test">${esc(r.test_name)}</div>
      <div class="run-dbms">${dbmsTags}</div>
      <div class="run-actions">${actions}</div>
    `;
    listEl.appendChild(card);
  });

  document.getElementById('count-label').textContent = `${count} run(s)`;

  if (count === 0) {
    listEl.innerHTML = '<div class="empty-state">No matching runs found.</div>';
  }
}

document.getElementById('filter-input').addEventListener('input', render);
document.getElementById('filter-dbms').addEventListener('change', render);
render();
</script>
</body>
</html>"""


def generate_index_html(output_dir: str):
    """Scan output_dir for historical runs and write index.html."""
    runs = _scan_runs(output_dir)
    page = _INDEX_TEMPLATE.replace(
        "{{RUNS_JSON}}", json.dumps(runs, ensure_ascii=False))
    index_path = os.path.join(output_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(page)
    log.info("History index written: %s", index_path)
