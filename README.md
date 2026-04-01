# Rosetta
Cross-DBMS SQL testing & benchmarking toolkit.

Rosetta executes SQL against multiple databases (TDSQL, MySQL, TiDB, OceanBase, etc.), compares behavioral consistency via MTR-style `.test` files, benchmarks query performance, and provides an interactive SQL playground — all with visual reports.

## Requirements
- Python >= 3.8
- PyMySQL >= 1.0
- Rich >= 13.0
- prompt_toolkit >= 3.0

## Installation
### Pip Install (Recommended)
```bash
git clone https://github.com/sjyango/rosetta.git
cd rosetta
pip install -e .
```
`rosetta` command is available globally after installation.

### Single File (.pyz)
```bash
./build.sh  # Output: dist/rosetta.pyz
pip install pymysql "rich>=13.0" "prompt_toolkit>=3.0"
python3 rosetta.pyz --help
```

## Quick Start
```bash
# 1. Generate config file
rosetta config init

# 2. Edit DB connection info
vim dbms_config.json

# 3. Check DB connectivity
rosetta status

# 4. Execute SQL across databases
rosetta exec --dbms tdsql,mysql --sql "SELECT VERSION()"

# 5. Run MTR consistency test
rosetta mtr --dbms tdsql,mysql -t test.test

# 6. Run performance benchmark
rosetta bench --dbms tdsql,mysql --file bench.json

# 7. Browse historical results
rosetta result list

# 8. Interactive mode (REPL)
rosetta i
```

## Usage
```bash
rosetta <command> [options]
```

### Global Options
All commands support these flags (can appear before or after the subcommand):

| Argument | Default | Description |
|----------|---------|-------------|
| `-j / --json` | `False` | JSON output (AI Agent friendly) |
| `-c / --config` | `dbms_config.json` | DBMS config file path |
| `-v / --verbose` | `False` | Enable verbose/debug logging |

### Commands

---

#### `status` — Check DB Connection Status
Check connectivity and version for all enabled databases in config.

```bash
rosetta status
rosetta status -j
rosetta status --timeout 10
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--timeout` | `5` | Connection timeout in seconds |

---

#### `exec` — Execute SQL (Playground)
Execute SQL statements across databases and compare results side-by-side.

```bash
# Execute single SQL
rosetta exec --dbms tdsql,mysql --sql "SELECT VERSION()"

# Execute SQL from file
rosetta exec --dbms tdsql,mysql --file queries.sql

# Execute on a specific database
rosetta exec --dbms mysql -d mydb --sql "SHOW TABLES"
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--sql` | one of `--sql` / `--file` | — | SQL statement to execute |
| `--file` | one of `--sql` / `--file` | — | File containing SQL statements |
| `--dbms` | ❌ | all enabled | DBMS targets (comma-separated) |
| `-d / --database` | ❌ | none | Database name (omit to connect without `USE`) |

---

#### `mtr` — MTR Consistency Test
Execute `.test` files and compare SQL execution results across databases. Generates HTML diff reports.

```bash
# Basic MTR test
rosetta mtr --dbms tdsql,mysql -t test.test

# With baseline comparison
rosetta mtr --dbms tdsql,mysql --baseline tdsql -t test.test

# Parse only (no execution)
rosetta mtr --dbms tdsql,mysql --parse-only -t test.test

# Serve HTML report after test
rosetta mtr --dbms tdsql,mysql --serve -t test.test
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `-t / --test` | ✅ | — | Path to `.test` file |
| `--dbms` | ✅ | — | DBMS targets (comma-separated) |
| `-b / --baseline` | ❌ | `tdsql` | Baseline DBMS for diff comparison |
| `-d / --database` | ❌ | `rosetta_mtr_test` | Test database name |
| `-o / --output-dir` | ❌ | `results` | Report output directory |
| `-f / --output-format` | ❌ | `all` | Report format: `text`, `html`, `all` |
| `--parse-only` | ❌ | `False` | Only parse `.test` file, no execution |
| `--diff-only` | ❌ | `False` | Re-generate reports from existing `.result` files |
| `-s / --serve` | ❌ | `False` | Start HTTP server to view reports |
| `-p / --port` | ❌ | `19527` | HTTP server port |

---

#### `bench` — Performance Benchmark
Compare query performance across databases with custom workloads. Supports serial and concurrent modes.

```bash
# Serial benchmark
rosetta bench --dbms tdsql,mysql --mode SERIAL --iterations 10 --file bench.json

# Concurrent benchmark (8 threads, 60s)
rosetta bench --dbms tdsql,mysql \
  --mode CONCURRENT --concurrency 8 --duration 60 --file bench.json

# Skip setup (reuse tables from previous run)
rosetta bench --dbms tdsql,mysql --skip-setup --file bench.json

# Disable flame graph capture
rosetta bench --dbms tdsql,mysql --no-profile --file bench.json
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--dbms` | ✅ | — | DBMS targets (comma-separated) |
| `--file` | ✅ | — | Benchmark definition file (`.json` / `.sql`) |
| `--mode` | ❌ | `SERIAL` | Execution mode: `SERIAL` or `CONCURRENT` |
| `-d / --database` | ❌ | `rosetta_bench_test` | Benchmark database name |
| `-o / --output-dir` | ❌ | `results` | Report output directory |
| `-f / --output-format` | ❌ | `all` | Report format: `text`, `html`, `all` |

**Serial mode** (`--mode SERIAL`):

| Argument | Default | Description |
|----------|---------|-------------|
| `--iterations` | `1` | Iterations per query |
| `--warmup` | `0` | Warmup iterations |

**Concurrent mode** (`--mode CONCURRENT`):

| Argument | Default | Description |
|----------|---------|-------------|
| `--concurrency` | `10` | Number of concurrent threads |
| `--duration` | `30` | Duration in seconds |
| `--warmup` | `0` | Warmup duration in seconds |
| `--ramp-up` | `0` | Ramp-up seconds for threads |

**Common options**:

| Argument | Default | Description |
|----------|---------|-------------|
| `--query-timeout` | `5` | Query timeout in seconds (0 = disabled) |
| `--bench-filter` | — | Run only queries matching these names (comma-separated) |
| `--repeat` | `1` | Number of benchmark rounds |
| `--skip-setup` | `False` | Skip setup phase (reuse existing tables) |
| `--skip-teardown` | `False` | Skip teardown (keep tables for next run) |
| `--no-parallel-dbms` | `False` | Run DBMS targets sequentially |
| `--no-profile` | `False` | Disable flame-graph capture |
| `--perf-freq` | `99` | perf sampling frequency in Hz |

---

#### `config` — Manage Configuration
View, validate, or generate DBMS configuration files.

```bash
# Generate sample config
rosetta config init

# Generate to custom path
rosetta config init --output my_config.json

# Show current config
rosetta config show

# Validate config (check JSON + connectivity)
rosetta config validate
```

| Action | Description |
|--------|-------------|
| `init` | Generate a sample `dbms_config.sample.json` |
| `show` | Display current config details |
| `validate` | Validate JSON structure and test connectivity |

---

#### `result` — Browse Historical Results
List, inspect, and navigate past MTR and benchmark runs.

```bash
# List runs (default: 20 per page)
rosetta result list

# Pagination
rosetta result list -n 10 -p 2

# Filter by type
rosetta result list --type bench
rosetta result list --type mtr

# Show details of latest run
rosetta result show

# Show a specific run (prefix match supported)
rosetta result show bench_json_mv_select_20260331

# JSON output
rosetta result show -j
```

**`result list` options**:

| Argument | Default | Description |
|----------|---------|-------------|
| `-n / --limit` | `20` | Rows per page |
| `-p / --page` | `1` | Page number |
| `--type` | `all` | Filter: `all`, `mtr`, `bench` |
| `-o / --output-dir` | `results` | Results directory |

**`result show` options**:

| Argument | Default | Description |
|----------|---------|-------------|
| `run_id` | latest | Run ID or prefix (optional) |
| `-o / --output-dir` | `results` | Results directory |

---

#### `i` / `repl` / `interactive` — Interactive REPL
Launch an interactive session for ad-hoc SQL execution, MTR tests, and benchmarks.

```bash
rosetta i
rosetta i --dbms tdsql,mysql
rosetta i --serve
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--dbms` | all enabled | DBMS targets (comma-separated) |
| `-d / --database` | `cross_dbms_test_db` | Test database name |
| `-o / --output-dir` | `results` | Report output directory |
| `-s / --serve` | `False` | Start HTTP server for reports |
| `-p / --port` | `19527` | HTTP server port |

---

## Configuration
Sample `dbms_config.json`:
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
      "enabled": true
    }
  ]
}
```

## Output Files
| File | Description |
|------|-------------|
| `<test_name>.html` | Interactive HTML report with side-by-side diff |
| `<test_name>.report.txt` | Text summary report |
| `bench_result.json` | Benchmark raw data (JSON) |
| `index.html` | History page of all test runs |
| `whitelist.json` | Persisted whitelist for acceptable diffs |
| `buglist.json` | Tracked known bugs from diff results |

## Testing
```bash
python -m pytest tests/test_cli.py -v
```

## Getting Help
```bash
rosetta --help
rosetta <command> --help
```
