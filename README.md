# Rosetta

Cross-DBMS SQL behavioral consistency verification tool.

Rosetta parses MySQL MTR-style `.test` files, executes the SQL statements against multiple database systems (TDSQL, MySQL, TiDB, OceanBase, etc.), compares execution results, and generates visual diff reports.

## Requirements

- Python >= 3.8
- PyMySQL >= 1.0
- Rich >= 13.0
- prompt_toolkit >= 3.0

## Installation

### 方式一：pip 安装（推荐）

```bash
git clone https://github.com/sjyango/rosetta.git
cd rosetta
pip install -e .
```

安装后 `rosetta` 命令全局可用。

### 方式二：.pyz 单文件

使用打包脚本构建一个可直接运行的单文件：

```bash
./build.sh
# 产出: dist/rosetta.pyz
```

运行时只需：

```bash
pip install pymysql "rich>=13.0" "prompt_toolkit>=3.0"
python3 rosetta.pyz --help
```

## Quick Start

```bash
# 1. Generate a sample config file
rosetta --gen-config dbms_config.json

# 2. Edit dbms_config.json with your DBMS connection info
vim dbms_config.json

# 3. Run a test
rosetta --test path/to/test.test --dbms tdsql,mysql

# 4. Run with HTTP server to view HTML reports
rosetta --test path/to/test.test --dbms tdsql,mysql --serve

# 5. Interactive mode (REPL, run multiple tests without restarting)
rosetta --interactive --dbms tdsql,mysql --serve
```

## Usage

```
rosetta --test <test_file> [options]
```

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--test, -t` | Path to MTR `.test` file |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--config, -c` | `dbms_config.json` | Path to DBMS config JSON file |
| `--dbms` | *(all enabled)* | DBMS to compare, comma-separated (e.g. `tdsql,mysql,tidb`) |
| `--baseline, -b` | `tdsql` | Baseline DBMS name for comparison |
| `--output-dir, -o` | `results` | Output directory for reports |
| `--format, -f` | `all` | Output format: `text`, `html`, or `all` |
| `--database, -d` | `cross_dbms_test_db` | Test database name |
| `--skip-explain` | `True` | Skip EXPLAIN statements |
| `--skip-analyze` | `False` | Skip ANALYZE TABLE statements |
| `--skip-show-create` | `False` | Skip SHOW CREATE TABLE statements |
| `--parse-only` | `False` | Only parse `.test` file and print statements (debug) |
| `--diff-only` | `False` | Re-generate reports from existing `.result` files without executing |
| `--gen-config` | — | Generate a sample config file at the given path and exit |
| `--serve, -s` | `False` | Start HTTP server after test run |
| `--port, -p` | `19527` | HTTP server port |
| `--interactive` | `False` | Enter interactive mode (REPL session) |
| `--verbose, -v` | `False` | Enable verbose logging |

## Examples

### Compare TDSQL and MySQL

```bash
rosetta --test suite/tdsql/json/t/test.test --dbms tdsql,mysql --baseline tdsql
```

### Compare all enabled DBMS (controlled by config file)

```bash
rosetta --test suite/tdsql/json/t/test.test
```

### Generate only HTML report

```bash
rosetta --test suite/tdsql/json/t/test.test --dbms tdsql,mysql --format html
```

### Re-generate reports from existing results (no DB connection needed)

```bash
rosetta --test suite/tdsql/json/t/test.test --diff-only --format html
```

### Start HTTP server to view reports

```bash
rosetta --test suite/tdsql/json/t/test.test --dbms tdsql,mysql --serve --port 8080
```

### Interactive mode (REPL session)

```bash
rosetta --interactive --config dbms_config.json --dbms tdsql,mysql --serve
```

In interactive mode, you get a REPL prompt where you can repeatedly submit `.test` file paths without restarting. Features include:

- **Tab completion** for `.test` file paths
- **Built-in HTTP server** with `--serve` for live report viewing
- **History page** (`index.html`) automatically updated after each run
- **Whitelist / Buglist management** via Web UI and REST API

### Parse-only mode (debug, no DB connection needed)

```bash
rosetta --test suite/tdsql/json/t/test.test --parse-only
```

## Configuration

The config file is a JSON file with the following structure:

```json
{
  "databases": [
    {
      "name": "tdsql",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "root",
      "password": "",
      "driver": "pymysql",
      "skip_patterns": [],
      "init_sql": [],
      "skip_explain": false,
      "skip_analyze": false,
      "skip_show_create": false,
      "enabled": true,
      "restart_cmd": ""
    }
  ]
}
```

### Config Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | DBMS identifier (used in `--dbms` and `--baseline`) |
| `host` | string | Database host |
| `port` | int | Database port |
| `user` | string | Database user |
| `password` | string | Database password |
| `driver` | string | Python DB driver (`pymysql`) |
| `skip_patterns` | list | SQL patterns to skip for this DBMS |
| `init_sql` | list | SQL statements to run on connection init |
| `skip_explain` | bool | Skip EXPLAIN statements for this DBMS |
| `skip_analyze` | bool | Skip ANALYZE TABLE statements for this DBMS |
| `skip_show_create` | bool | Skip SHOW CREATE TABLE statements for this DBMS |
| `enabled` | bool | Whether this DBMS is included when `--dbms` is not specified |
| `restart_cmd` | string | Command to restart the DBMS (used on connection failure) |

## Adding a New DBMS

1. Add a new entry in your `dbms_config.json`:

```json
{
  "name": "new_dbms",
  "host": "127.0.0.1",
  "port": 3307,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "skip_patterns": [],
  "init_sql": ["SET ..."],
  "enabled": true
}
```

2. Run with `--dbms` to include it:

```bash
rosetta --test test.test --dbms tdsql,mysql,new_dbms
```

No code changes required — any MySQL-protocol-compatible DBMS can be added via configuration alone.

## Output

Rosetta generates the following files in the output directory:

| File | Description |
|------|-------------|
| `<test_name>.<dbms>.result` | Raw execution output per DBMS |
| `<test_name>.report.txt` | Text summary report |
| `<test_name>.diff` | Unified diff output |
| `<test_name>.html` | Interactive HTML report with dashboard and side-by-side diff |
| `index.html` | History page listing all test runs |
| `whitelist.json` | Persisted whitelist entries (auto-created) |
| `buglist.json` | Persisted buglist entries (auto-created) |
| `whitelist.html` | Whitelist management page |
| `buglist.html` | Buglist management page |

## Whitelist & Buglist

Rosetta supports **whitelisting** and **bug-marking** individual diff blocks. This helps manage known differences across test runs.

### Whitelist

Whitelisted diffs are **excluded from the failure count** — they no longer cause a test to be reported as FAIL. Use this for known acceptable differences (e.g., DBMS-specific behavior that is correct but different).

- Each diff is identified by an **MD5 fingerprint** computed from the normalised SQL statement and the outputs of both DBMS.
- Whitelist entries are persisted in `whitelist.json` in the output directory.
- Whitelisted diffs appear with reduced opacity and a yellow "Whitelisted" badge in the HTML report.

### Buglist

Bug-marked diffs are **informational only** — they still count toward the failure rate, but are visually distinguished so you can track known bugs across runs.

- Bug entries are persisted in `buglist.json` in the output directory.
- Bug-marked diffs appear with a red left border and a "Bug" badge in the HTML report.

### Managing via HTML Report

In the interactive HTML report for each test run, every diff block has action buttons:

- **加白 (Whitelist)** — add the diff to the whitelist; click again to remove
- **标记Bug (Mark Bug)** — mark the diff as a known bug; click again to remove

Changes take effect immediately and are persisted to the JSON files. When reopening a report from the history page, the whitelist/buglist state is synced from the server via API.

### Managing via REST API

When running with `--serve`, the following API endpoints are available:

| Endpoint | Method | Action | Body |
|----------|--------|--------|------|
| `/api/whitelist/list` | POST | List all whitelist entries | `{}` |
| `/api/whitelist/add` | POST | Add a whitelist entry | `{"fingerprint": "...", "stmt": "...", "dbms_a": "...", "dbms_b": "...", "block": 0, "reason": ""}` |
| `/api/whitelist/remove` | POST | Remove a whitelist entry | `{"fingerprint": "..."}` |
| `/api/whitelist/clear` | POST | Clear all whitelist entries | `{}` |
| `/api/buglist/list` | POST | List all buglist entries | `{}` |
| `/api/buglist/add` | POST | Add a buglist entry | `{"fingerprint": "...", "stmt": "...", "dbms_a": "...", "dbms_b": "...", "block": 0, "reason": ""}` |
| `/api/buglist/remove` | POST | Remove a buglist entry | `{"fingerprint": "..."}` |
| `/api/buglist/clear` | POST | Clear all buglist entries | `{}` |

## Project Structure

```
rosetta/
├── pyproject.toml         # Package metadata and console_scripts entry
├── setup.py               # Fallback install entry
├── build.sh               # Build script for .pyz packaging
├── dbms_config.sample.json # Sample DBMS config file
└── rosetta/               # Python package
    ├── __init__.py        # Package definition
    ├── __main__.py        # python -m rosetta entry point
    ├── models.py          # Data models (Statement, StmtResult, DBMSConfig, CompareResult)
    ├── config.py          # Config loading, validation, sample generation
    ├── parser.py          # MTR .test file parser
    ├── executor.py        # DB connection management and SQL execution
    ├── comparator.py      # Result normalization and diff comparison
    ├── whitelist.py       # Whitelist management (MD5 fingerprint, JSON persistence)
    ├── buglist.py         # Buglist management (known bug tracking)
    ├── interactive.py     # Interactive REPL session with HTTP server
    ├── reporter/
    │   ├── __init__.py
    │   ├── text.py        # Text report generator
    │   ├── html.py        # HTML visual report generator (with whitelist/buglist UI)
    │   └── history.py     # History index page and whitelist/buglist management pages
    ├── ui.py              # Terminal UI helpers (summary table, progress)
    └── cli.py             # CLI entry point and orchestration
```
