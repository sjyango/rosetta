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

# ---------------------------------------------------------------------------
# Inline SVG Logos
# ---------------------------------------------------------------------------

# Playground Logo — terminal/play icon style
_PLAYGROUND_LOGO_SVG = (
    '<svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">'
    '<rect x="1" y="4" width="30" height="24" rx="4" stroke="#3fb950" stroke-width="2" fill="none"/>'
    '<rect x="1" y="4" width="30" height="8" rx="4" fill="#21262d"/>'
    '<circle cx="7" cy="8" r="1.5" fill="#f85149"/>'
    '<circle cx="12" cy="8" r="1.5" fill="#d29922"/>'
    '<circle cx="17" cy="8" r="1.5" fill="#3fb950"/>'
    '<path d="M11 20L15 23L11 26Z" fill="#3fb950"/>'
    '<line x1="18" y1="22" x2="25" y2="22" stroke="#58a6ff" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="26" x2="22" y2="26" stroke="#8b949e" stroke-width="1.5" stroke-linecap="round"/>'
    '</svg>'
)

# MTR / History Logo — stone tablet / comparison icon
_MTR_LOGO_SVG = (
    '<svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">'
    '<path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" '
    'stroke="#58a6ff" stroke-width="2" fill="none"/>'
    '<line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/>'
    '<line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="20" x2="23" y2="20" stroke="#d29922" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '<line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/>'
    '</svg>'
)

# Pattern: <test_name>_YYYYMMDD_HHMMSS
_RUN_DIR_RE = re.compile(r"^(.+)_(\d{8}_\d{6})$")
# Pattern for benchmark: bench_<workload>_YYYYMMDD_HHMMSS
_BENCH_DIR_RE = re.compile(r"^bench_(.+)_(\d{8}_\d{6})$")


def _scan_runs(output_dir: str) -> List[dict]:
    """Return a list of run metadata dicts, newest first."""
    runs = []
    for entry in os.listdir(output_dir):
        full = os.path.join(output_dir, entry)
        if not os.path.isdir(full):
            continue

        # Check if this is a benchmark run
        bm = _BENCH_DIR_RE.match(entry)
        is_bench = bm is not None

        m = _RUN_DIR_RE.match(entry)
        if not m:
            continue

        test_name = m.group(1)
        stamp = m.group(2)  # YYYYMMDD_HHMMSS

        # Look for HTML report
        html_file = None
        if is_bench:
            # Benchmark HTML uses bench_<workload>.html
            workload_name = bm.group(1)
            candidate = f"bench_{workload_name}.html"
            if os.path.isfile(os.path.join(full, candidate)):
                html_file = candidate
        if html_file is None:
            # Standard test report
            candidate = f"{test_name}.html"
            if os.path.isfile(os.path.join(full, candidate)):
                html_file = candidate

        has_html = html_file is not None
        html_path = os.path.join(full, html_file) if html_file else ""

        # Look for text report to extract summary
        report_file = f"{test_name}.report.txt"
        if is_bench:
            report_file = f"bench_{bm.group(1)}.report.txt"
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

        # For benchmark runs, try to extract DBMS names from JSON
        if is_bench and not dbms_names:
            json_path = os.path.join(full, "bench_result.json")
            if os.path.isfile(json_path):
                try:
                    with open(json_path, "r", encoding="utf-8") as jf:
                        jdata = json.load(jf)
                    dbms_names = [
                        dr["dbms_name"]
                        for dr in jdata.get("dbms_results", [])
                    ]
                except Exception:
                    pass

        run_type = "benchmark" if is_bench else "test"
        workload = bm.group(1) if is_bench else test_name

        runs.append({
            "dir_name": entry,
            "test_name": test_name,
            "workload": workload,
            "stamp": stamp,
            "display_time": display_time,
            "has_html": has_html,
            "html_link": f"{entry}/{html_file}" if has_html else "",
            "report_link": f"{entry}/{report_file}" if os.path.isfile(report_path) else "",
            "dbms": dbms_names,
            "summary": summary_line,
            "run_type": run_type,
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
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 28px; display: flex; align-items: center; gap: 8px; }
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
.run-time { color: var(--fg2); font-size: 13px; width: 155px; flex-shrink: 0; font-family: 'SF Mono', Consolas, monospace; }
.run-id { font-size: 12px; width: 210px; flex-shrink: 0; font-family: 'SF Mono', Consolas, monospace;
  color: var(--fg2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.run-workload { font-weight: 600; font-size: 14px; width: 150px; flex-shrink: 0;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.run-dbms { display: flex; gap: 6px; flex-wrap: wrap; flex: 1; align-items: center; }
.dbms-tag { background: var(--bg3); border: 1px solid var(--border);
  border-radius: 4px; padding: 2px 8px; font-size: 12px; color: var(--fg2); }
.run-actions { display: flex; gap: 8px; }
.btn { display: inline-block; padding: 6px 14px; border-radius: 6px;
  font-size: 13px; font-weight: 500; text-decoration: none; cursor: pointer; border: none; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-primary:hover { opacity: 0.9; }
.btn-secondary { background: var(--bg3); color: var(--fg2); border: 1px solid var(--border); }
.btn-secondary:hover { color: var(--fg); border-color: var(--fg2); }
.btn-danger { background: var(--red-bg); color: var(--red); border: 1px solid var(--red); }
.btn-danger:hover { opacity: 0.85; }
.empty-state { text-align: center; padding: 60px 20px; color: var(--fg2); font-size: 16px; }
.modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6);
  display: flex; align-items: center; justify-content: center; z-index: 1000; }
.modal-content { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
  padding: 24px; max-width: 400px; }
.modal-content h3 { margin-bottom: 12px; }
.modal-content p { margin-bottom: 20px; color: var(--fg2); }
.modal-actions { display: flex; gap: 12px; justify-content: flex-end; }
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1><svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" stroke="#58a6ff" stroke-width="2" fill="none"/><line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/><line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="20" x2="23" y2="20" stroke="#d29922" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/></svg> Rosetta History</h1>
    <a href="playground.html" style="color:var(--green);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9654; Playground</a>
    <a href="whitelist.html" style="color:var(--yellow);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9782; Whitelist</a>
    <a href="buglist.html" style="color:var(--red);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#128027; Buglist</a>
  </div>
  <div class="subtitle">Cross-DBMS SQL behavioral consistency verification</div>

  <div class="toolbar">
    <input type="text" id="filter-input" placeholder="Filter by test name...">
    <select id="filter-dbms"><option value="">All DBMS</option></select>
    <span class="count-label" id="count-label"></span>
  </div>

  <div id="run-list"></div>
</div>

<!-- Delete confirmation modal -->
<div id="delete-modal" class="modal" style="display:none">
  <div class="modal-content">
    <h3>Delete Run</h3>
    <p>Are you sure you want to delete <strong id="delete-target-name"></strong>?</p>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal()">Cancel</button>
      <button class="btn btn-danger" onclick="deleteRun()">Delete</button>
    </div>
  </div>
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
    const typeBadge = r.run_type === 'benchmark'
      ? '<span class="dbms-tag" style="background:var(--green-bg);color:var(--green);border-color:var(--green)">Benchmark</span>'
      : '';

    let actions = '';
    if (r.has_html) {
      actions += `<a class="btn btn-primary" href="${esc(r.html_link)}">View Report</a>`;
    }
    if (r.report_link) {
      actions += `<a class="btn btn-secondary" href="${esc(r.report_link)}">Text Report</a>`;
    }
    actions += `<button class="btn btn-danger" onclick="confirmDelete('${esc(r.dir_name)}')">Delete</button>`;

    card.innerHTML = `
      <div class="run-time">${esc(r.display_time)}</div>
      <div class="run-id" title="${esc(r.dir_name)}">${esc(r.dir_name)}</div>
      <div class="run-workload" title="${esc(r.workload)}">${esc(r.workload)}</div>
      <div class="run-dbms">${typeBadge}${dbmsTags}</div>
      <div class="run-actions">${actions}</div>
    `;
    listEl.appendChild(card);
  });

  document.getElementById('count-label').textContent = `${count} run(s)`;

  if (count === 0) {
    listEl.innerHTML = '<div class="empty-state">No matching runs found.</div>';
  }
}

let deleteTarget = null;

function confirmDelete(dirName) {
  deleteTarget = dirName;
  document.getElementById('delete-modal').style.display = 'flex';
  document.getElementById('delete-target-name').textContent = dirName;
}

function closeModal() {
  document.getElementById('delete-modal').style.display = 'none';
  deleteTarget = null;
}

async function deleteRun() {
  if (!deleteTarget) return;
  const btn = document.querySelector('#delete-modal .btn-danger');
  if (btn) btn.disabled = true;
  try {
    const res = await fetch('/api/runs/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dir_name: deleteTarget })
    });
    if (res.ok) {
      closeModal();
      location.reload();
    } else {
      const err = await res.text();
      alert('Delete failed: ' + err);
      if (btn) btn.disabled = false;
    }
  } catch (e) {
    alert('Delete error: ' + e.message);
    if (btn) btn.disabled = false;
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


# ---------------------------------------------------------------------------
# Whitelist management page
# ---------------------------------------------------------------------------

_WHITELIST_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta — Whitelist</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --orange: #db8b0b; --orange-bg: #2d2009;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 28px; display: flex; align-items: center; gap: 8px; }
.subtitle { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }

.toolbar { display: flex; gap: 12px; margin-bottom: 20px; align-items: center; flex-wrap: wrap; }
.toolbar input {
  background: var(--bg2); border: 1px solid var(--border); border-radius: 6px;
  padding: 8px 14px; color: var(--fg); font-size: 14px; outline: none; width: 400px; }
.toolbar input:focus { border-color: var(--accent); }
.count-label { color: var(--fg2); font-size: 14px; margin-left: auto; }
.btn { display: inline-block; padding: 8px 16px; border-radius: 6px;
  font-size: 13px; font-weight: 500; text-decoration: none; cursor: pointer;
  border: 1px solid var(--border); }
.btn-danger { background: var(--red-bg); color: var(--red); border-color: var(--red); }
.btn-danger:hover { opacity: 0.85; }
.btn-secondary { background: var(--bg3); color: var(--fg2); }
.btn-secondary:hover { color: var(--fg); border-color: var(--fg2); }
.btn-nav { color: var(--blue); font-size: 14px; border: 1px solid var(--border);
  border-radius: 6px; padding: 4px 12px; background: none; text-decoration: none; }

.wl-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px 20px; margin-bottom: 12px;
  transition: border-color 0.15s; }
.wl-card:hover { border-color: var(--accent); }
.wl-card-header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }
.wl-fp { font-family: 'SF Mono', Consolas, monospace; font-size: 12px;
  color: var(--fg2); background: var(--bg3); padding: 2px 8px; border-radius: 4px; }
.wl-dbms { font-size: 13px; color: var(--orange); }
.wl-time { font-size: 12px; color: var(--fg2); margin-left: auto; }
.wl-stmt { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); margin-bottom: 8px; word-break: break-all; }
.wl-reason { font-size: 13px; color: var(--fg2); margin-bottom: 8px; }
.wl-actions { display: flex; gap: 8px; }

.empty-state { text-align: center; padding: 60px 20px; color: var(--fg2); font-size: 16px; }

.toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
  border-radius: 8px; font-size: 14px; color: #fff; z-index: 9999;
  transition: opacity 0.3s; pointer-events: none; }
.toast-success { background: var(--green); }
.toast-error { background: var(--red); }

.modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6);
  z-index: 9998; justify-content: center; align-items: center; }
.modal-overlay.active { display: flex; }
.modal { background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
  padding: 24px; max-width: 460px; width: 90%; }
.modal h3 { margin-bottom: 12px; }
.modal p { color: var(--fg2); font-size: 14px; margin-bottom: 20px; }
.modal-actions { display: flex; gap: 12px; justify-content: flex-end; }
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1><svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" stroke="#58a6ff" stroke-width="2" fill="none"/><line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/><line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="20" x2="23" y2="20" stroke="#d29922" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/></svg> Whitelist</h1>
    <a href="index.html" class="btn-nav">&#9664; History</a>
    <a href="playground.html" style="color:var(--green);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9654; Playground</a>
  </div>
  <div class="subtitle">Manage whitelisted diffs — these are excluded from mismatch counts</div>

  <div class="toolbar">
    <input type="text" id="filter-input" placeholder="Filter by SQL statement or fingerprint...">
    <button class="btn btn-danger" id="btn-clear-all">Clear All</button>
    <span class="count-label" id="count-label"></span>
  </div>

  <div id="wl-list"></div>
</div>

<div class="modal-overlay" id="confirm-modal">
  <div class="modal">
    <h3>Clear all whitelist entries?</h3>
    <p>This will remove all whitelisted diffs. This action cannot be undone.</p>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal()">Cancel</button>
      <button class="btn btn-danger" id="btn-confirm-clear">Yes, clear all</button>
    </div>
  </div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
let WL_DATA = {};  // populated via API

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast toast-' + (type || 'success');
  t.style.opacity = '1';
  setTimeout(() => { t.style.opacity = '0'; }, 2500);
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function callAPI(action, body) {
  const port = location.port || '80';
  const base = location.protocol + '//' + location.hostname + ':' + port;
  return fetch(base + '/api/whitelist/' + action, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  }).then(r => r.json());
}

function loadAndRender() {
  callAPI('list', {}).then(r => {
    if (r.ok) { WL_DATA = r.entries || {}; render(); }
    else { showToast('Failed to load whitelist', 'error'); }
  }).catch(e => {
    showToast('API error: ' + e.message, 'error');
  });
}

function render() {
  const filter = document.getElementById('filter-input').value.toLowerCase();
  const listEl = document.getElementById('wl-list');
  listEl.innerHTML = '';

  const entries = Object.entries(WL_DATA);
  let count = 0;

  entries.forEach(([fp, entry]) => {
    const matchText = (entry.stmt || '') + ' ' + fp + ' ' + (entry.dbms_a || '') + ' ' + (entry.dbms_b || '');
    if (filter && !matchText.toLowerCase().includes(filter)) return;
    count++;

    const card = document.createElement('div');
    card.className = 'wl-card';
    card.id = 'wl-' + fp;
    card.innerHTML = `
      <div class="wl-card-header">
        <span class="wl-fp">${fp.substring(0,12)}…</span>
        <span class="wl-dbms">${esc(entry.dbms_a || '?')} vs ${esc(entry.dbms_b || '?')}</span>
        ${entry.block ? '<span style="color:var(--fg2);font-size:12px">Block ' + entry.block + '</span>' : ''}
        <span class="wl-time">${esc(entry.added_at || '')}</span>
      </div>
      <div class="wl-stmt">${esc(entry.stmt || '(no statement)')}</div>
      ${entry.reason ? '<div class="wl-reason">Reason: ' + esc(entry.reason) + '</div>' : ''}
      <div class="wl-actions">
        <button class="btn btn-danger" onclick="removeEntry('${fp}')">Remove</button>
      </div>
    `;
    listEl.appendChild(card);
  });

  document.getElementById('count-label').textContent = count + ' entry(ies)';

  if (count === 0) {
    listEl.innerHTML = '<div class="empty-state">' +
      (entries.length === 0 ? 'Whitelist is empty.' : 'No entries match the filter.') +
      '</div>';
  }
}

function removeEntry(fp) {
  callAPI('remove', {fingerprint: fp}).then(r => {
    if (r.ok) {
      delete WL_DATA[fp];
      render();
      showToast('Entry removed', 'success');
    } else {
      showToast('Failed: ' + (r.error || 'unknown'), 'error');
    }
  });
}

function closeModal() {
  document.getElementById('confirm-modal').classList.remove('active');
}

document.getElementById('btn-clear-all').onclick = () => {
  document.getElementById('confirm-modal').classList.add('active');
};

document.getElementById('btn-confirm-clear').onclick = () => {
  closeModal();
  callAPI('clear', {}).then(r => {
    if (r.ok) { WL_DATA = {}; render(); showToast('Whitelist cleared', 'success'); }
    else { showToast('Failed: ' + (r.error || 'unknown'), 'error'); }
  });
};

document.getElementById('filter-input').addEventListener('input', render);

// Initial load
loadAndRender();
</script>
</body>
</html>"""


def generate_whitelist_html(output_dir: str):
    """Generate whitelist management page."""
    page = _WHITELIST_TEMPLATE
    wl_path = os.path.join(output_dir, "whitelist.html")
    with open(wl_path, "w", encoding="utf-8") as f:
        f.write(page)
    log.info("Whitelist page written: %s", wl_path)


# ---------------------------------------------------------------------------
# Bug list management page
# ---------------------------------------------------------------------------

_BUGLIST_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta — Buglist</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --purple: #a371f7; --purple-bg: #1e163b;
  --border: #30363d; --accent: #1f6feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { color: var(--fg); margin-bottom: 4px; font-size: 28px; display: flex; align-items: center; gap: 8px; }
.subtitle { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }

.toolbar { display: flex; gap: 12px; margin-bottom: 20px; align-items: center; flex-wrap: wrap; }
.toolbar input {
  background: var(--bg2); border: 1px solid var(--border); border-radius: 6px;
  padding: 8px 14px; color: var(--fg); font-size: 14px; outline: none; width: 400px; }
.toolbar input:focus { border-color: var(--accent); }
.count-label { color: var(--fg2); font-size: 14px; margin-left: auto; }
.btn { display: inline-block; padding: 8px 16px; border-radius: 6px;
  font-size: 13px; font-weight: 500; text-decoration: none; cursor: pointer;
  border: 1px solid var(--border); }
.btn-danger { background: var(--red-bg); color: var(--red); border-color: var(--red); }
.btn-danger:hover { opacity: 0.85; }
.btn-secondary { background: var(--bg3); color: var(--fg2); }
.btn-secondary:hover { color: var(--fg); border-color: var(--fg2); }
.btn-nav { color: var(--blue); font-size: 14px; border: 1px solid var(--border);
  border-radius: 6px; padding: 4px 12px; background: none; text-decoration: none; }

.bl-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px 20px; margin-bottom: 12px;
  border-left: 3px solid var(--red); transition: border-color 0.15s; }
.bl-card:hover { border-color: var(--accent); border-left-color: var(--red); }
.bl-card-header { display: flex; align-items: center; gap: 16px; margin-bottom: 8px; }
.bl-fp { font-family: 'SF Mono', Consolas, monospace; font-size: 12px;
  color: var(--fg2); background: var(--bg3); padding: 2px 8px; border-radius: 4px; }
.bl-dbms { font-size: 13px; color: var(--red); }
.bl-time { font-size: 12px; color: var(--fg2); margin-left: auto; }
.bl-stmt { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); margin-bottom: 8px; word-break: break-all; }
.bl-reason { font-size: 13px; color: var(--fg2); margin-bottom: 8px; }
.bl-actions { display: flex; gap: 8px; }

.empty-state { text-align: center; padding: 60px 20px; color: var(--fg2); font-size: 16px; }

.toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
  border-radius: 8px; font-size: 14px; color: #fff; z-index: 9999;
  transition: opacity 0.3s; pointer-events: none; }
.toast-success { background: var(--green); }
.toast-error { background: var(--red); }

.modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6);
  z-index: 9998; justify-content: center; align-items: center; }
.modal-overlay.active { display: flex; }
.modal { background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
  padding: 24px; max-width: 460px; width: 90%; }
.modal h3 { margin-bottom: 12px; }
.modal p { color: var(--fg2); font-size: 14px; margin-bottom: 20px; }
.modal-actions { display: flex; gap: 12px; justify-content: flex-end; }
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1><svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" stroke="#58a6ff" stroke-width="2" fill="none"/><line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/><line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="20" x2="23" y2="20" stroke="#d29922" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/></svg> Buglist</h1>
    <a href="index.html" class="btn-nav">&#9664; History</a>
    <a href="playground.html" style="color:var(--green);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9654; Playground</a>
    <a href="whitelist.html" style="color:var(--yellow);font-size:14px;text-decoration:none;border:1px solid var(--border);border-radius:6px;padding:4px 12px">&#9782; Whitelist</a>
  </div>
  <div class="subtitle">Manage bug-marked diffs — these still count toward the failure rate</div>

  <div class="toolbar">
    <input type="text" id="filter-input" placeholder="Filter by SQL statement or fingerprint...">
    <button class="btn btn-danger" id="btn-clear-all">Clear All</button>
    <span class="count-label" id="count-label"></span>
  </div>

  <div id="bl-list"></div>
</div>

<div class="modal-overlay" id="confirm-modal">
  <div class="modal">
    <h3>Clear all bug list entries?</h3>
    <p>This will remove all bug marks. This action cannot be undone.</p>
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal()">Cancel</button>
      <button class="btn btn-danger" id="btn-confirm-clear">Yes, clear all</button>
    </div>
  </div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
let BL_DATA = {};  // populated via API

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast toast-' + (type || 'success');
  t.style.opacity = '1';
  setTimeout(() => { t.style.opacity = '0'; }, 2500);
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function callAPI(action, body) {
  const port = location.port || '80';
  const base = location.protocol + '//' + location.hostname + ':' + port;
  return fetch(base + '/api/buglist/' + action, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  }).then(r => r.json());
}

function loadAndRender() {
  callAPI('list', {}).then(r => {
    if (r.ok) { BL_DATA = r.entries || {}; render(); }
    else { showToast('Failed to load buglist', 'error'); }
  }).catch(e => {
    showToast('API error: ' + e.message, 'error');
  });
}

function render() {
  const filter = document.getElementById('filter-input').value.toLowerCase();
  const listEl = document.getElementById('bl-list');
  listEl.innerHTML = '';

  const entries = Object.entries(BL_DATA);
  let count = 0;

  entries.forEach(([fp, entry]) => {
    const matchText = (entry.stmt || '') + ' ' + fp + ' ' + (entry.dbms_a || '') + ' ' + (entry.dbms_b || '');
    if (filter && !matchText.toLowerCase().includes(filter)) return;
    count++;

    const card = document.createElement('div');
    card.className = 'bl-card';
    card.id = 'bl-' + fp;
    card.innerHTML = `
      <div class="bl-card-header">
        <span class="bl-fp">${fp.substring(0,12)}\u2026</span>
        <span class="bl-dbms">${esc(entry.dbms_a || '?')} vs ${esc(entry.dbms_b || '?')}</span>
        ${entry.block ? '<span style="color:var(--fg2);font-size:12px">Block ' + entry.block + '</span>' : ''}
        <span class="bl-time">${esc(entry.added_at || '')}</span>
      </div>
      <div class="bl-stmt">${esc(entry.stmt || '(no statement)')}</div>
      ${entry.reason ? '<div class="bl-reason">Reason: ' + esc(entry.reason) + '</div>' : ''}
      <div class="bl-actions">
        <button class="btn btn-danger" onclick="removeEntry('${fp}')">Remove</button>
      </div>
    `;
    listEl.appendChild(card);
  });

  document.getElementById('count-label').textContent = count + ' entry(ies)';

  if (count === 0) {
    listEl.innerHTML = '<div class="empty-state">' +
      (entries.length === 0 ? 'Buglist is empty.' : 'No entries match the filter.') +
      '</div>';
  }
}

function removeEntry(fp) {
  callAPI('remove', {fingerprint: fp}).then(r => {
    if (r.ok) {
      delete BL_DATA[fp];
      render();
      showToast('Entry removed', 'success');
    } else {
      showToast('Failed: ' + (r.error || 'unknown'), 'error');
    }
  });
}

function closeModal() {
  document.getElementById('confirm-modal').classList.remove('active');
}

document.getElementById('btn-clear-all').onclick = () => {
  document.getElementById('confirm-modal').classList.add('active');
};

document.getElementById('btn-confirm-clear').onclick = () => {
  closeModal();
  callAPI('clear', {}).then(r => {
    if (r.ok) { BL_DATA = {}; render(); showToast('Buglist cleared', 'success'); }
    else { showToast('Failed: ' + (r.error || 'unknown'), 'error'); }
  });
};

document.getElementById('filter-input').addEventListener('input', render);

// Initial load
loadAndRender();
</script>
</body>
</html>"""


def generate_buglist_html(output_dir: str):
    """Generate buglist management page."""
    page = _BUGLIST_TEMPLATE
    bl_path = os.path.join(output_dir, "buglist.html")
    with open(bl_path, "w", encoding="utf-8") as f:
        f.write(page)
    log.info("Buglist page written: %s", bl_path)


# ---------------------------------------------------------------------------
# SQL Playground page
# ---------------------------------------------------------------------------

_PLAYGROUND_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta — SQL Playground</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --orange: #db8b0b;
  --border: #30363d; --accent: #1f6feb;
  --diff-add: #1a4721; --diff-del: #5b2125;
  --glow: rgba(31,111,235,0.25);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; }

/* Scrollbar — dark theme */
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: var(--bg); border-radius: 6px; }
::-webkit-scrollbar-thumb { background: var(--bg3); border-radius: 6px;
  border: 2px solid var(--bg); }
::-webkit-scrollbar-thumb:hover { background: var(--fg2); }
::-webkit-scrollbar-corner { background: var(--bg); }
.sql-input::-webkit-scrollbar { width: 8px; }
.sql-input::-webkit-scrollbar-track { background: var(--bg); border-radius: 0 10px 10px 0; }
.sql-input::-webkit-scrollbar-thumb { background: var(--bg3); border-radius: 6px;
  border: 2px solid var(--bg); }
.sql-input::-webkit-scrollbar-thumb:hover { background: var(--fg2); }
/* Firefox scrollbar */
* { scrollbar-width: thin; scrollbar-color: var(--bg3) var(--bg); }

/* Header */
.header { padding: 14px 28px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 16px;
  background: linear-gradient(180deg, var(--bg2) 0%, var(--bg) 100%); }
.header h1 { font-size: 22px; display: flex; align-items: center; gap: 8px; }
.header h1 .icon { font-size: 18px; color: var(--green); }
.btn-nav { color: var(--fg2); font-size: 13px; border: 1px solid var(--border);
  border-radius: 6px; padding: 5px 14px; background: var(--bg3); text-decoration: none;
  transition: all 0.15s; }
.btn-nav:hover { border-color: var(--blue); color: var(--blue); background: var(--bg2); }

/* Layout */
.main { display: flex; flex-direction: column; height: calc(100vh - 56px); overflow: auto; }

/* Input area */
.input-area { padding: 20px 28px 0; border-bottom: none; }
.input-area-inner { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 20px; overflow: hidden; }
.input-label { font-size: 12px; color: var(--fg2); text-transform: uppercase;
  letter-spacing: 0.5px; font-weight: 600; margin-bottom: 8px; display: flex;
  align-items: center; gap: 6px; }
.input-label .hint { text-transform: none; font-weight: 400; letter-spacing: 0; }
.input-row { display: flex; gap: 14px; align-items: stretch; }
.sql-input { flex: 1; background: var(--bg); border: 1px solid var(--border);
  border-radius: 10px; color: var(--fg); padding: 16px 20px; font-size: 14px;
  font-family: 'SF Mono', Consolas, 'Courier New', monospace;
  min-height: 160px; max-height: 400px; resize: vertical; outline: none;
  line-height: 1.7; transition: border-color 0.2s, box-shadow 0.2s;
  tab-size: 2; }
.sql-input:focus { border-color: var(--accent);
  box-shadow: 0 0 0 3px var(--glow); }
.sql-input::placeholder { color: var(--fg2); opacity: 0.6; }
.right-controls { display: flex; flex-direction: column; gap: 10px;
  justify-content: flex-start; min-width: 120px; }
.btn-exec { background: linear-gradient(135deg, var(--accent), #388bfd);
  color: #fff; border: none; border-radius: 10px; padding: 14px 28px;
  font-size: 15px; font-weight: 600; cursor: pointer; white-space: nowrap;
  transition: all 0.2s; box-shadow: 0 2px 8px rgba(31,111,235,0.3); }
.btn-exec:hover { transform: translateY(-1px);
  box-shadow: 0 4px 16px rgba(31,111,235,0.4); }
.btn-exec:active { transform: translateY(0); }
.btn-exec:disabled { opacity: 0.5; cursor: not-allowed; transform: none;
  box-shadow: none; }
.btn-stop { background: linear-gradient(135deg, #d1242f, #f85149);
  color: #fff; border: none; border-radius: 10px; padding: 14px 28px;
  font-size: 15px; font-weight: 600; cursor: pointer; white-space: nowrap;
  transition: all 0.2s; box-shadow: 0 2px 8px rgba(248,81,73,0.3);
  display: none; }
.btn-stop:hover { transform: translateY(-1px);
  box-shadow: 0 4px 16px rgba(248,81,73,0.4); }
.btn-stop:active { transform: translateY(0); }
.btn-stop:disabled { opacity: 0.5; cursor: not-allowed; transform: none;
  box-shadow: none; }
.btn-clear { background: var(--bg3); color: var(--fg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 8px 16px; font-size: 13px; cursor: pointer;
  transition: all 0.15s; }
.btn-clear:hover { color: var(--fg); border-color: var(--fg2);
  background: var(--bg); }
.shortcut-hint { font-size: 11px; color: var(--fg2); text-align: center;
  opacity: 0.7; }
.shortcut-hint kbd { background: var(--bg3); border: 1px solid var(--border);
  border-radius: 3px; padding: 1px 5px; font-size: 10px;
  font-family: inherit; }

/* DBMS selector */
.dbms-selector { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap;
  align-items: center; }
.dbms-selector label { font-size: 12px; color: var(--fg2); margin-right: 4px;
  text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
.dbms-chip { display: inline-flex; align-items: center; gap: 6px;
  background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
  padding: 6px 14px; font-size: 13px; cursor: pointer; user-select: none;
  transition: all 0.2s; font-weight: 500; }
.dbms-chip .cb { display: inline-flex; align-items: center; justify-content: center;
  width: 16px; height: 16px; border: 2px solid var(--border); border-radius: 4px;
  background: var(--bg); flex-shrink: 0; transition: all 0.15s;
  font-size: 10px; color: transparent; line-height: 1; }
.dbms-chip.active { background: rgba(31,111,235,0.12); border-color: var(--accent);
  color: var(--blue); }
.dbms-chip.active .cb { background: var(--accent); border-color: var(--accent);
  color: #fff; }
.dbms-chip:hover { border-color: var(--accent); }
.dbms-chip .host-info { font-size: 11px; color: var(--fg2); margin-left: 2px; }
.dbms-actions { display: inline-flex; gap: 4px; margin-left: 4px; }
.dbms-actions button { background: none; border: 1px solid var(--border);
  border-radius: 6px; color: var(--fg2); font-size: 11px; padding: 3px 10px;
  cursor: pointer; transition: all 0.15s; }
.dbms-actions button:hover { color: var(--fg); border-color: var(--fg2); }
.dbms-count { font-size: 11px; color: var(--fg2); margin-left: 8px;
  background: var(--bg3); padding: 3px 10px; border-radius: 10px; }
.db-info { font-size: 12px; color: var(--fg2); margin-left: 8px;
  background: var(--bg3); padding: 4px 10px; border-radius: 12px; }

/* Results area */
.results-area { flex: 1; padding: 20px 28px; }

/* Per-statement result block */
.stmt-block { margin-bottom: 28px; }
.stmt-sql { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px 16px; margin-bottom: 12px;
  word-break: break-all; border-left: 3px solid var(--accent); }
.stmt-label { font-size: 12px; color: var(--fg2); margin-bottom: 4px;
  font-weight: 600; }

/* Grid of DBMS result panels — always same row */
.results-grid { display: flex; gap: 14px; overflow-x: auto; }
.results-grid > .result-panel { flex: 1 1 0; min-width: 280px; }
.result-panel { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; overflow: hidden; transition: border-color 0.2s; }
.result-panel:hover { border-color: var(--fg2); }
.result-panel.has-diff { border-color: var(--red);
  box-shadow: 0 0 0 1px var(--red-bg); }

.result-panel-header { padding: 10px 16px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  background: var(--bg3); }
.result-panel-name { font-weight: 600; font-size: 14px;
  display: flex; align-items: center; gap: 8px; }
.result-panel-meta { font-size: 12px; color: var(--fg2); }
.result-panel-body { padding: 0; overflow-x: auto; }

/* Data table */
.data-table { width: 100%; border-collapse: collapse; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; }
.data-table th { background: var(--bg3); padding: 8px 14px; text-align: left;
  font-weight: 600; color: var(--fg2); border-bottom: 1px solid var(--border);
  white-space: nowrap; }
.data-table td { padding: 6px 14px; border-bottom: 1px solid var(--border);
  white-space: nowrap; }
.data-table tr:last-child td { border-bottom: none; }
.data-table tr:hover td { background: rgba(255,255,255,0.02); }
.cell-diff { background: var(--diff-del); border-radius: 3px; padding: 1px 5px; }
.cell-match { }

/* Error / info states */
.result-error { color: var(--red); padding: 14px 16px; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; }
.result-error-match { color: var(--orange); padding: 14px 16px; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; }
.result-ok { color: var(--green); padding: 14px 16px; font-size: 13px; }
.result-empty { color: var(--fg2); padding: 14px 16px; font-size: 13px; }

/* Diff summary badge */
.diff-badge { display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 11px; font-weight: 600; }
.diff-badge-match { background: var(--green-bg); color: var(--green); }
.diff-badge-diff { background: var(--red-bg); color: var(--red); }
.diff-badge-skip { background: #2a2518; color: var(--yellow); }
.diff-badge-baseline { background: #12261e; color: var(--green); }

/* Results summary bar */
.results-summary { display: flex; gap: 10px; align-items: center; padding: 10px 20px;
  background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
  margin-bottom: 16px; font-size: 13px; font-weight: 600; }
.summary-btn { display: inline-flex; align-items: center; gap: 4px;
  background: none; border: 1px solid transparent; border-radius: 6px;
  padding: 4px 10px; cursor: pointer; font-size: 13px; font-weight: 600;
  transition: all 0.15s; }
.summary-btn:hover { background: var(--bg3); border-color: var(--border); }
.summary-btn.active { background: var(--bg3); border-color: var(--accent);
  box-shadow: 0 0 0 1px var(--accent); }
.summary-match { color: var(--green); }
.summary-match.active { background: var(--green-bg); border-color: var(--green); box-shadow: none; }
.summary-diff { color: var(--red); }
.summary-diff.active { background: var(--red-bg); border-color: var(--red); box-shadow: none; }
.summary-diff-zero { color: var(--fg2); opacity: 0.6; cursor: default; background: none; border: none; padding: 4px 10px; }
.summary-skip { color: var(--yellow); }
.summary-skip.active { background: #2a2518; border-color: var(--yellow); box-shadow: none; }
.summary-total { color: var(--fg2); margin-left: auto; font-weight: 400; }
.summary-total.active { background: var(--bg3); border-color: var(--fg2); box-shadow: none; }

/* Loading */
.loading { text-align: center; padding: 60px; color: var(--fg2); font-size: 15px; }
.loading .spinner { display: inline-block; width: 28px; height: 28px;
  border: 3px solid var(--border); border-top-color: var(--accent);
  border-radius: 50%; animation: spin 0.8s linear infinite; margin-right: 12px;
  vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }

/* Progress bar */
.progress-container { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; padding: 24px; margin-bottom: 16px; }
.progress-header { display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 16px; }
.progress-title { font-size: 14px; font-weight: 600; color: var(--fg);
  display: flex; align-items: center; gap: 10px; }
.progress-title .spinner { width: 18px; height: 18px; border: 2px solid var(--border);
  border-top-color: var(--accent); border-radius: 50%;
  animation: spin 0.8s linear infinite; }
.progress-stats { font-size: 13px; color: var(--fg2); }
.progress-bar-track { width: 100%; height: 6px; background: var(--bg3);
  border-radius: 3px; overflow: hidden; }
.progress-bar-fill { height: 100%; background: linear-gradient(90deg, var(--accent), #388bfd);
  border-radius: 3px; transition: width 0.3s ease; width: 0%; }
.progress-dbms-list { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
.progress-dbms-item { display: inline-flex; align-items: center; gap: 6px;
  font-size: 12px; padding: 4px 10px; border-radius: 6px;
  background: var(--bg3); color: var(--fg2); border: 1px solid var(--border);
  transition: all 0.2s; }
.progress-dbms-item.done { background: rgba(63,185,80,0.12);
  border-color: var(--green); color: var(--green); }
.progress-dbms-item.error { background: rgba(248,81,73,0.12);
  border-color: var(--red); color: var(--red); }
.progress-dbms-item.running { background: rgba(31,111,235,0.12);
  border-color: var(--accent); color: var(--blue); }
.progress-dbms-item .dbms-status-icon { font-size: 11px; }

/* Empty state */
.empty-state { text-align: center; padding: 100px 20px; color: var(--fg2); }
.empty-state .icon-big { font-size: 48px; margin-bottom: 16px; opacity: 0.4; }
.empty-state h2 { color: var(--fg); margin-bottom: 10px; font-size: 22px;
  font-weight: 600; }
.empty-state p { font-size: 14px; line-height: 1.8; }
.empty-state kbd { background: var(--bg3); border: 1px solid var(--border);
  border-radius: 4px; padding: 2px 8px; font-size: 12px; }

/* Toast */
.toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
  border-radius: 10px; font-size: 14px; color: #fff; z-index: 9999;
  transition: opacity 0.3s; pointer-events: none;
  box-shadow: 0 4px 20px rgba(0,0,0,0.4); }
.toast-error { background: var(--red); }
</style>
</head>
<body>

<div class="header">
  <h1>""" + _PLAYGROUND_LOGO_SVG + r""" SQL Playground</h1>
  <a href="index.html" class="btn-nav">&#9664; History</a>
  <a href="whitelist.html" class="btn-nav">&#9782; Whitelist</a>
  <a href="buglist.html" class="btn-nav">&#128027; Buglist</a>
</div>

<div class="main">
  <div class="input-area">
    <div class="input-area-inner">
    <div class="input-label">SQL Editor <span class="hint">— enter one or more statements separated by ;</span></div>
    <div class="input-row">
      <textarea class="sql-input" id="sql-input"
        placeholder="SELECT 1 + 1;&#10;SHOW DATABASES;&#10;CREATE TABLE t(id INT);&#10;INSERT INTO t VALUES(1);&#10;SELECT * FROM t;"
        spellcheck="false"></textarea>
      <div class="right-controls">
        <button class="btn-exec" id="btn-exec" onclick="executeSql()">&#9654; Execute</button>
        <button class="btn-stop" id="btn-stop" onclick="stopExecution()">&#9632; Stop</button>
        <button class="btn-clear" onclick="clearResults()">Clear</button>
        <span class="shortcut-hint"><kbd>Ctrl</kbd>+<kbd>Enter</kbd></span>
      </div>
    </div>
    <div class="dbms-selector" id="dbms-selector">
      <label>Targets:</label>
      <!-- chips populated by JS -->
    </div>
    <div class="dbms-selector" id="baseline-selector">
      <label>Baseline:</label>
      <!-- chips populated by JS -->
    </div>
    </div>
  </div>

  <div class="results-area" id="results-area">
    <div class="empty-state">
      <div class="icon-big">&#128640;</div>
      <h2>Ready to execute</h2>
      <p>Enter SQL above and click <b>Execute</b> or press <kbd>Ctrl+Enter</kbd></p>
    </div>
  </div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
let DBMS_LIST = [];
let ACTIVE_DBMS = new Set();
let BASELINE_DBMS = '';
let DATABASE = '';
let _currentAbortController = null;

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast toast-' + (type || 'error');
  t.style.opacity = '1';
  setTimeout(() => { t.style.opacity = '0'; }, 3000);
}

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function fmtElapsed(ms) {
  if (ms < 1) return ms.toFixed(3) + ' ms';
  if (ms < 1000) return ms.toFixed(1) + ' ms';
  return (ms / 1000).toFixed(2) + ' s';
}

function apiCall(method, path, body) {
  const port = location.port || '80';
  const base = location.protocol + '//' + location.hostname + ':' + port;
  const opts = { method, headers: {'Content-Type': 'application/json'} };
  if (body) opts.body = JSON.stringify(body);
  return fetch(base + path, opts).then(r => r.json());
}

// ---- DBMS chips ----
function loadDbms() {
  apiCall('GET', '/api/dbms').then(r => {
    if (!r.ok) { showToast('Failed to load DBMS list'); return; }
    DBMS_LIST = r.dbms || [];
    DATABASE = r.database || '';
    ACTIVE_DBMS = new Set(DBMS_LIST.filter(d => d.active).map(d => d.name));
    if (!BASELINE_DBMS && DBMS_LIST.length > 0) {
      BASELINE_DBMS = DBMS_LIST[0].name;
    }
    renderChips();
    renderBaselineChips();
  }).catch(e => showToast('API error: ' + e.message));
}

function renderChips() {
  const container = document.getElementById('dbms-selector');
  container.innerHTML = '<label>Targets:</label>';
  DBMS_LIST.forEach(d => {
    const chip = document.createElement('span');
    chip.className = 'dbms-chip' + (ACTIVE_DBMS.has(d.name) ? ' active' : '');
    chip.innerHTML = '<span class="cb">' + (ACTIVE_DBMS.has(d.name) ? '&#10003;' : '') + '</span>' +
      esc(d.name) + '<span class="host-info">' + esc(d.host + ':' + d.port) + '</span>';
    chip.title = d.host + ':' + d.port;
    chip.onclick = () => {
      if (ACTIVE_DBMS.has(d.name)) ACTIVE_DBMS.delete(d.name);
      else ACTIVE_DBMS.add(d.name);
      renderChips();
    };
    container.appendChild(chip);
  });
  if (DBMS_LIST.length > 1) {
    const actions = document.createElement('span');
    actions.className = 'dbms-actions';
    const btnAll = document.createElement('button');
    btnAll.textContent = 'All';
    btnAll.onclick = () => { ACTIVE_DBMS = new Set(DBMS_LIST.map(d => d.name)); renderChips(); };
    const btnNone = document.createElement('button');
    btnNone.textContent = 'None';
    btnNone.onclick = () => { ACTIVE_DBMS.clear(); renderChips(); };
    actions.appendChild(btnAll);
    actions.appendChild(btnNone);
    container.appendChild(actions);
  }
  const count = document.createElement('span');
  count.className = 'dbms-count';
  count.textContent = ACTIVE_DBMS.size + ' / ' + DBMS_LIST.length + ' selected';
  container.appendChild(count);
  if (DATABASE) {
    const info = document.createElement('span');
    info.className = 'db-info';
    info.textContent = 'DB: ' + DATABASE;
    container.appendChild(info);
  }
}

// ---- Baseline selector (radio-style chips) ----
function renderBaselineChips() {
  const container = document.getElementById('baseline-selector');
  container.innerHTML = '<label>Baseline:</label>';
  DBMS_LIST.forEach(d => {
    const chip = document.createElement('span');
    chip.className = 'dbms-chip' + (d.name === BASELINE_DBMS ? ' active' : '');
    // Radio: filled circle when active, hollow when not
    chip.innerHTML = '<span class="cb">' + (d.name === BASELINE_DBMS ? '&#9679;' : '') + '</span>' +
      esc(d.name) + '<span class="host-info">' + esc(d.host + ':' + d.port) + '</span>';
    chip.title = d.host + ':' + d.port;
    chip.onclick = () => {
      BASELINE_DBMS = (BASELINE_DBMS === d.name) ? '' : d.name;
      renderBaselineChips();
    };
    container.appendChild(chip);
  });
}

function onBaselineChange() {
  // kept for API compatibility — no longer needed
}

// ---- Execute ----
function executeSql() {
  const sql = document.getElementById('sql-input').value.trim();
  if (!sql) return;
  if (ACTIVE_DBMS.size === 0) { showToast('Select at least one DBMS'); return; }

  const btn = document.getElementById('btn-exec');
  const stopBtn = document.getElementById('btn-stop');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner" style="width:16px;height:16px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:6px"></span>Running...';
  stopBtn.style.display = 'block';
  stopBtn.disabled = false;

  const area = document.getElementById('results-area');
  area.innerHTML = '';

  const dbmsList = [...ACTIVE_DBMS];
  const total = dbmsList.length;
  let completed = 0;
  const results = {};

  // Create progress UI
  const progressContainer = document.createElement('div');
  progressContainer.className = 'progress-container';
  progressContainer.innerHTML =
    '<div class="progress-header">' +
      '<div class="progress-title"><span class="spinner"></span>Executing SQL...</div>' +
      '<div class="progress-stats"><span id="progress-completed">0</span>/' + total + ' completed</div>' +
    '</div>' +
    '<div class="progress-bar-track"><div class="progress-bar-fill" id="progress-fill"></div></div>' +
    '<div class="progress-dbms-list" id="progress-dbms-list">' +
      dbmsList.map(n => '<div class="progress-dbms-item" id="progress-item-' + esc(n) + '">' +
        '<span class="dbms-status-icon">&#9679;</span>' + esc(n) + '</div>').join('') +
    '</div>';
  area.appendChild(progressContainer);

  // Use SSE for streaming results
  const port = location.port || '80';
  const base = location.protocol + '//' + location.hostname + ':' + port;

  const abortCtrl = new AbortController();
  _currentAbortController = abortCtrl;

  fetch(base + '/api/execute/stream', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ sql: sql, dbms: dbmsList }),
    signal: abortCtrl.signal
  }).then(response => {
    if (!response.ok) {
      throw new Error('HTTP ' + response.status);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    function processChunk() {
      return reader.read().then(({ done, value }) => {
        if (done) {
          // Stream ended — finalize
          finalizeExecution();
          return;
        }

        buffer += decoder.decode(value, { stream: true });

        // Parse SSE events from buffer
        const parts = buffer.split('\n\n');
        buffer = parts.pop() || '';  // keep incomplete part

        for (const part of parts) {
          let event = 'message';
          let data = '';
          for (const line of part.split('\n')) {
            if (line.startsWith('event: ')) {
              event = line.substring(7);
            } else if (line.startsWith('data: ')) {
              data = line.substring(6);
            }
          }

          if (!data) continue;

          try {
            const parsed = JSON.parse(data);

            if (event === 'progress') {
              completed++;
              results[parsed.name] = parsed.result;

              // Update progress bar
              const pct = Math.round((completed / total) * 100);
              document.getElementById('progress-fill').style.width = pct + '%';
              document.getElementById('progress-completed').textContent = completed;

              // Update DBMS item status
              const item = document.getElementById('progress-item-' + parsed.name);
              if (item) {
                if (parsed.result.error) {
                  item.className = 'progress-dbms-item error';
                  item.querySelector('.dbms-status-icon').innerHTML = '&#10007;';
                } else {
                  item.className = 'progress-dbms-item done';
                  item.querySelector('.dbms-status-icon').innerHTML = '&#10003;';
                }
              }
            } else if (event === 'done') {
              finalizeExecution();
            } else if (event === 'cancelled') {
              showToast('Execution cancelled by user', 'error');
              finalizeExecution();
            } else if (event === 'error') {
              showToast('Execution error: ' + (parsed.error || 'Unknown'));
              finalizeExecution();
            }
          } catch (e) {
            // ignore parse errors
          }
        }

        return processChunk();
      });
    }

    processChunk().catch(e => {
      if (e.name === 'AbortError') {
        showToast('Execution cancelled', 'error');
      } else {
        showToast('Stream error: ' + e.message);
      }
      finalizeExecution();
    });
  }).catch(e => {
    if (e.name === 'AbortError') {
      showToast('Execution cancelled', 'error');
    } else {
      showToast('Request failed: ' + e.message);
    }
    finalizeExecution();
  });

  let finalized = false;
  function finalizeExecution() {
    if (finalized) return;
    finalized = true;
    _currentAbortController = null;
    btn.disabled = false;
    btn.innerHTML = '&#9654; Execute';
    stopBtn.style.display = 'none';
    stopBtn.disabled = true;
    stopBtn.innerHTML = '&#9632; Stop';

    if (Object.keys(results).length > 0) {
      renderResults(results, sql);
    } else {
      // No results (e.g. cancelled before any DBMS responded) — clear progress
      area.innerHTML = '<div class="empty-state"><p>Execution was cancelled. No results.</p></div>';
    }
  }
}

function stopExecution() {
  const stopBtn = document.getElementById('btn-stop');
  stopBtn.disabled = true;
  stopBtn.innerHTML = '&#9632; Stopping...';

  const port = location.port || '80';
  const base = location.protocol + '//' + location.hostname + ':' + port;

  // 1. Tell backend to kill active DB connections
  fetch(base + '/api/stop', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({})
  }).catch(() => {});

  // 2. Abort the SSE fetch stream so the UI unblocks immediately
  if (_currentAbortController) {
    _currentAbortController.abort();
    _currentAbortController = null;
  }
}

// ---- Render results ----
function renderResults(results, originalSql) {
  const area = document.getElementById('results-area');
  area.innerHTML = '';

  let dbmsNames = [...ACTIVE_DBMS].filter(n => results[n]);
  if (dbmsNames.length === 0) {
    area.innerHTML = '<div class="empty-state"><p>No results returned.</p></div>';
    return;
  }

  // Sort: baseline DBMS first, then the rest
  if (BASELINE_DBMS && dbmsNames.includes(BASELINE_DBMS)) {
    dbmsNames = [BASELINE_DBMS, ...dbmsNames.filter(n => n !== BASELINE_DBMS)];
  }

  // Check for connection-level errors
  const connErrors = dbmsNames.filter(n => results[n].error);
  const okNames = dbmsNames.filter(n => !results[n].error);

  // Determine the reference DBMS for diff: baseline if set and OK, else first OK
  const refName = (BASELINE_DBMS && okNames.includes(BASELINE_DBMS))
    ? BASELINE_DBMS
    : okNames[0];

  // Get max statement count
  const maxStmts = Math.max(...dbmsNames.map(n => (results[n].statements || []).length), 0);

  // Build statement display list from the reference DBMS's actual SQL
  // (backend TestFileParser already filtered out --echo, --error, comments, etc.)
  const stmtDisplayList = refName && results[refName] && results[refName].statements
    ? results[refName].statements.map(s => s.sql || '')
    : [];

  // Count match/diff/skip-diff across all statements
  let matchCount = 0, diffCount = 0, skipDiffCount = 0;
  const nonRefNames = dbmsNames.filter(n => n !== refName);

  for (let si = 0; si < maxStmts; si++) {
    const stmtSql = stmtDisplayList[si] || '';
    const skipDiff = isSkipDiff(stmtSql);

    if (skipDiff) {
      skipDiffCount++;
      continue;
    }

    // Build statement results for comparison
    const stmtResults = {};
    dbmsNames.forEach(n => {
      const r = results[n];
      if (r.error) {
        stmtResults[n] = { type: 'conn_error', error: r.error };
      } else if (r.statements && r.statements[si]) {
        stmtResults[n] = r.statements[si];
      } else {
        stmtResults[n] = { type: 'missing' };
      }
    });
    const refResult = refName ? stmtResults[refName] : null;

    // Check if any non-ref DBMS has a diff for this statement
    let stmtHasDiff = false;
    if (refResult) {
      for (const name of nonRefNames) {
        if (hasDiff(refResult, stmtResults[name])) {
          stmtHasDiff = true;
          break;
        }
      }
    }
    if (stmtHasDiff) {
      diffCount++;
    } else {
      matchCount++;
    }
  }

  // Render summary bar
  if (maxStmts > 0 && nonRefNames.length > 0) {
    const summaryBar = document.createElement('div');
    summaryBar.className = 'results-summary';
    summaryBar.id = 'results-summary-bar';
    summaryBar.innerHTML =
      '<button class="summary-btn summary-match" data-filter="match" title="Show only matched statements">&#10003; Match: ' + matchCount + '</button>' +
      (diffCount > 0
        ? '<button class="summary-btn summary-diff" data-filter="diff" title="Show only diff statements">&#10007; Diff: ' + diffCount + '</button>'
        : '<span class="summary-btn summary-diff-zero">Diff: 0</span>') +
      (skipDiffCount > 0
        ? '<button class="summary-btn summary-skip" data-filter="skip-diff" title="Show only skip-diff statements">&#9888; Skip-Diff: ' + skipDiffCount + '</button>'
        : '') +
      '<button class="summary-btn summary-total" data-filter="" title="Show all statements">Total: ' + maxStmts + '</button>';
    area.appendChild(summaryBar);

    // Filter logic
    summaryBar.querySelectorAll('.summary-btn[data-filter]').forEach(btn => {
      btn.addEventListener('click', () => {
        const filter = btn.getAttribute('data-filter');
        // Update active state
        summaryBar.querySelectorAll('.summary-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        // Show/hide statement blocks
        area.querySelectorAll('.stmt-block').forEach(block => {
          if (!filter) {
            block.style.display = '';
          } else {
            block.style.display = block.getAttribute('data-status') === filter ? '' : 'none';
          }
        });
      });
    });
  }

  // Render each statement
  for (let si = 0; si < maxStmts; si++) {
    const block = document.createElement('div');
    block.className = 'stmt-block';

    // SQL label — use the actual SQL from backend, not a client-side split
    const sqlDiv = document.createElement('div');
    sqlDiv.className = 'stmt-sql';
    sqlDiv.innerHTML = (maxStmts > 1 ? '<span class="stmt-label">Statement ' + (si+1) + '/' + maxStmts + '</span> ' : '') +
      esc(stmtDisplayList[si] || '');
    block.appendChild(sqlDiv);

    // Compute diff info for this statement across all DBMS
    const stmtResults = {};
    dbmsNames.forEach(n => {
      const r = results[n];
      if (r.error) {
        stmtResults[n] = { type: 'conn_error', error: r.error };
      } else if (r.statements && r.statements[si]) {
        stmtResults[n] = r.statements[si];
      } else {
        stmtResults[n] = { type: 'missing' };
      }
    });

    // Reference result from the chosen baseline
    const refResult = refName ? stmtResults[refName] : null;

    // Check if this statement type should skip diff validation
    const stmtSql = stmtDisplayList[si] || '';
    const skipDiff = isSkipDiff(stmtSql);

    // Determine statement-level diff status for filtering
    let stmtHasDiff = false;
    if (!skipDiff && refResult) {
      for (const name of nonRefNames) {
        const sr = stmtResults[name];
        if (hasDiff(refResult, sr)) {
          stmtHasDiff = true;
          break;
        }
      }
    }

    // Grid
    const grid = document.createElement('div');
    grid.className = 'results-grid';

    dbmsNames.forEach(name => {
      const sr = stmtResults[name];
      const panel = document.createElement('div');
      panel.className = 'result-panel';

      let isDiff = false;
      if (skipDiff) {
        // No special panel styling — keep default border
      } else {
        isDiff = refResult && name !== refName && hasDiff(refResult, sr);
        if (isDiff) panel.classList.add('has-diff');
      }

      // Header
      const header = document.createElement('div');
      header.className = 'result-panel-header';
      let badge = '';
      if (name === refName) {
        badge = ' <span class="diff-badge diff-badge-baseline">BASELINE</span>';
      } else if (skipDiff) {
        badge = ' <span class="diff-badge diff-badge-skip">SKIP-DIFF</span>';
      } else if (refResult) {
        badge = isDiff
          ? ' <span class="diff-badge diff-badge-diff">DIFF</span>'
          : ' <span class="diff-badge diff-badge-match">MATCH</span>';
      }
      header.innerHTML = '<span class="result-panel-name">' + esc(name) + badge + '</span>' +
        '<span class="result-panel-meta">' +
        (sr.elapsed_ms != null ? '<span style="color:var(--fg2);font-weight:600">' + fmtElapsed(sr.elapsed_ms) + '</span>' : '') +
        (sr.columns && sr.rows ? ' · ' + sr.rows.length + ' row(s)' :
         !sr.error && !sr.type ? ' · ' + (sr.affected_rows || 0) + ' affected' : '') +
        '</span>';
      panel.appendChild(header);

      // Body
      const body = document.createElement('div');
      body.className = 'result-panel-body';

      if (sr.type === 'conn_error') {
        body.innerHTML = '<div class="result-error">Connection error: ' + esc(sr.error) + '</div>';
      } else if (sr.type === 'missing') {
        body.innerHTML = '<div class="result-empty">No result</div>';
      } else if (sr.error) {
        // Use different style for matched errors (same error code across DBMS)
        const errorClass = (!skipDiff && isDiff) ? 'result-error' : 'result-error-match';
        body.innerHTML = '<div class="' + errorClass + '">' + esc(sr.error) + '</div>';
      } else if (sr.columns && sr.rows) {
        const table = buildTable(sr, (!skipDiff && name !== refName && refResult) ? refResult : null);
        body.appendChild(table);
      } else {
        body.innerHTML = '<div class="result-ok">OK — ' + (sr.affected_rows || 0) + ' row(s) affected</div>';
      }

      panel.appendChild(body);
      grid.appendChild(panel);
    });

    block.appendChild(grid);
    area.appendChild(block);

    // Set data-status for filtering
    if (skipDiff) {
      block.setAttribute('data-status', 'skip-diff');
    } else if (stmtHasDiff) {
      block.setAttribute('data-status', 'diff');
    } else {
      block.setAttribute('data-status', 'match');
    }
  }
}

function hasDiff(a, b) {
  if (!a || !b) return true;
  if (a.error && !b.error) return true;
  if (!a.error && b.error) return true;
  // When both have errors, compare error_code instead of error message
  // because error messages may differ across DBMS but error codes are aligned
  if (a.error && b.error) {
    // If both have error_code, compare by error_code
    if (a.error_code != null && b.error_code != null) {
      return a.error_code !== b.error_code;
    }
    // Fallback to comparing error messages if error_code is not available
    return a.error !== b.error;
  }
  // Compare columns
  if (JSON.stringify(a.columns) !== JSON.stringify(b.columns)) return true;
  // Compare rows
  if (JSON.stringify(a.rows) !== JSON.stringify(b.rows)) return true;
  // Compare affected_rows for non-SELECT
  if (!a.columns && !b.columns && a.affected_rows !== b.affected_rows) return true;
  return false;
}

// SQL statement types that skip diff validation — output-only, no cross-DBMS comparison
const SKIP_DIFF_PATTERNS = [
  /^\s*ANALYZE\s+/i,
  /^\s*EXPLAIN\s+/i,
  /^\s*SET\s+/i,
  /^\s*SHOW\s+/i,
  /^\s*FLUSH\s+/i,
  /^\s*RESET\s+/i,
  /^\s*OPTIMIZE\s+/i,
  /^\s*CHECKSUM\s+/i,
  /^\s*CHECK\s+/i,
  /^\s*REPAIR\s+/i,
  /^\s*GRANT\s+/i,
  /^\s*REVOKE\s+/i,
  /^\s*LOCK\s+/i,
  /^\s*UNLOCK\s+/i,
];

function isSkipDiff(sql) {
  if (!sql) return false;
  return SKIP_DIFF_PATTERNS.some(p => p.test(sql));
}

function buildTable(sr, refSr) {
  const table = document.createElement('table');
  table.className = 'data-table';

  // Header
  const thead = document.createElement('thead');
  const hRow = document.createElement('tr');
  (sr.columns || []).forEach((col, ci) => {
    const th = document.createElement('th');
    const refCol = refSr && refSr.columns ? refSr.columns[ci] : col;
    if (refSr && col !== refCol) {
      th.innerHTML = '<span class="cell-diff">' + esc(col) + '</span>';
    } else {
      th.textContent = col;
    }
    hRow.appendChild(th);
  });
  thead.appendChild(hRow);
  table.appendChild(thead);

  // Body
  const tbody = document.createElement('tbody');
  (sr.rows || []).forEach((row, ri) => {
    const tr = document.createElement('tr');
    row.forEach((cell, ci) => {
      const td = document.createElement('td');
      const refRow = refSr && refSr.rows ? refSr.rows[ri] : null;
      const refCell = refRow ? refRow[ci] : cell;
      if (refSr && String(cell) !== String(refCell)) {
        td.innerHTML = '<span class="cell-diff">' + esc(cell) + '</span>';
      } else {
        td.textContent = cell;
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  return table;
}

// ---- Clear ----
function clearResults() {
  document.getElementById('sql-input').value = '';
  document.getElementById('results-area').innerHTML =
    '<div class="empty-state"><h2>Ready to execute</h2>' +
    '<p>Enter SQL above and click <b>Execute</b> or press <kbd>Ctrl+Enter</kbd></p></div>';
}

// ---- Keyboard shortcut ----
document.getElementById('sql-input').addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    e.preventDefault();
    executeSql();
  }
});

// ---- Init ----
loadDbms();
</script>
</body>
</html>"""


def generate_playground_html(output_dir: str):
    """Generate SQL Playground page."""
    page = _PLAYGROUND_TEMPLATE
    pg_path = os.path.join(output_dir, "playground.html")
    with open(pg_path, "w", encoding="utf-8") as f:
        f.write(page)
    log.info("Playground page written: %s", pg_path)
