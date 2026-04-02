# Rosetta Commands Reference

Complete reference for all rosetta commands with examples and best practices.

## Global Options

All commands support these global flags:

| Argument | Default | Description |
|----------|---------|-------------|
| `-j / --json` | `False` | JSON output (AI Agent friendly) |
| `-c / --config` | `dbms_config.json` | DBMS config file path |
| `-v / --verbose` | `False` | Enable verbose/debug logging |
| `--version` | `False` | Show version and exit |

**Example:**
```bash
# JSON output for programmatic processing
rosetta status -j

# Use custom config file
rosetta status -c /path/to/config.json

# Verbose mode for debugging
rosetta status -v
```

---

## status — Check DB Connection Status

Check connectivity and version for all enabled databases in config.

### Usage
```bash
rosetta status [options]
```

### Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--timeout` | `5` | Connection timeout in seconds |

### Examples

```bash
# Basic status check
rosetta status

# JSON output for automation
rosetta status -j

# Extended timeout for slow networks
rosetta status --timeout 10
```

### Output

```
Database Connection Status
┏━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ DBMS   ┃ Status   ┃ Version ┃ Message                ┃
┡━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ mysql  │ ✓ OK     │ 8.0.26  │ Connected successfully │
│ tdsql  │ ✓ OK     │ 10.3.9  │ Connected successfully │
│ tidb   │ ✗ FAILED │ -       │ Connection refused     │
└────────┴──────────┴─────────┴─────────────────────────┘
```

---

## exec — Execute SQL Statements

Execute SQL statements across databases and compare results side-by-side.

### Usage
```bash
rosetta exec --sql "SQL_STATEMENT" --dbms DBMS_LIST [options]
rosetta exec --file SQL_FILE --dbms DBMS_LIST [options]
```

### Options

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--sql` | One of sql/file | — | SQL statement to execute |
| `--file` | One of sql/file | — | File containing SQL statements |
| `--dbms` | ❌ | all enabled | DBMS targets (comma-separated) |
| `-d / --database` | ❌ | none | Database name |

### Examples

```bash
# Execute single SQL statement
rosetta exec --dbms mysql,tdsql --sql "SELECT VERSION()"

# Execute on specific database
rosetta exec --dbms mysql -d mydb --sql "SHOW TABLES"

# Execute from file
rosetta exec --dbms mysql,tdsql --file queries.sql

# Compare results in JSON format
rosetta exec --dbms mysql,tdsql --sql "SELECT 1+1" -j
```

### Use Cases

- Quick SQL testing across databases
- Comparing query results
- Debugging SQL differences
- Validating schema changes

---

## mtr — MTR Consistency Test

Execute `.test` files and compare SQL execution results across databases. Generates HTML diff reports.

### Usage
```bash
rosetta mtr --dbms DBMS_LIST -t TEST_FILE [options]
```

### Options

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
| `--skip-explain` | ❌ | `True` | Skip EXPLAIN statements |
| `--skip-analyze` | ❌ | `False` | Skip ANALYZE TABLE statements |
| `--skip-show-create` | ❌ | `False` | Skip SHOW CREATE TABLE statements |

### Examples

```bash
# Basic MTR test
rosetta mtr --dbms mysql,tdsql -t test.test

# With specific baseline
rosetta mtr --dbms mysql,tdsql,tidb --baseline mysql -t test.test

# Parse only (validate test file)
rosetta mtr --dbms mysql,tdsql --parse-only -t test.test

# Generate HTML report and serve
rosetta mtr --dbms mysql,tdsql -t test.test --serve

# Re-generate reports from existing results
rosetta mtr --dbms mysql,tdsql --diff-only -t test.test

# Custom output directory
rosetta mtr --dbms mysql,tdsql -t test.test -o my_results
```

### Test File Format

`.test` files follow MySQL MTR format:

```sql
--echo Test 1: Basic SELECT
SELECT 1+1;

--echo Test 2: Create table
CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100));
INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM t1 ORDER BY id;
DROP TABLE t1;

--echo Test 3: Transaction
START TRANSACTION;
INSERT INTO t1 VALUES (3, 'Charlie');
ROLLBACK;
```

### Output Files

- `<test_name>.result` - Raw execution results
- `<test_name>.diff` - Differences found
- `<test_name>.html` - Interactive HTML report
- `<test_name>.report.txt` - Text summary

---

## bench — Performance Benchmark

Compare query performance across databases with custom workloads.

### Usage
```bash
rosetta bench --dbms DBMS_LIST --file BENCH_FILE [options]
```

### Options

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--dbms` | ✅ | — | DBMS targets (comma-separated) |
| `--file` | ✅ | — | Benchmark definition file (`.json` / `.sql`) |
| `--mode` | ❌ | `SERIAL` | Execution mode: `SERIAL` or `CONCURRENT` |
| `-d / --database` | ❌ | `rosetta_bench_test` | Benchmark database name |
| `-o / --output-dir` | ❌ | `results` | Report output directory |
| `-f / --output-format` | ❌ | `all` | Report format: `text`, `html`, `all` |

**Serial mode options:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--iterations` | `1` | Iterations per query |
| `--warmup` | `0` | Warmup iterations |

**Concurrent mode options:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--concurrency` | `10` | Number of concurrent threads |
| `--duration` | `30` | Duration in seconds |
| `--warmup` | `0` | Warmup duration in seconds |
| `--ramp-up` | `0` | Ramp-up seconds for threads |

**Common options:**

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

### Examples

```bash
# Serial benchmark (default)
rosetta bench --dbms mysql,tdsql --file bench.json

# Multiple iterations with warmup
rosetta bench --dbms mysql,tdsql --file bench.json \
  --iterations 10 --warmup 2

# Concurrent benchmark
rosetta bench --dbms mysql,tdsql --file bench.json \
  --mode CONCURRENT --concurrency 8 --duration 60

# Run specific queries only
rosetta bench --dbms mysql,tdsql --file bench.json \
  --bench-filter "query1,query2"

# Reuse tables from previous run
rosetta bench --dbms mysql,tdsql --file bench.json --skip-setup

# JSON output for analysis
rosetta bench --dbms mysql,tdsql --file bench.json -j
```

### Benchmark File Format

JSON format:
```json
{
  "setup": [
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
    "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
  ],
  "queries": [
    {
      "name": "select_all",
      "sql": "SELECT * FROM users",
      "description": "Select all users"
    },
    {
      "name": "select_by_id",
      "sql": "SELECT * FROM users WHERE id = 1",
      "description": "Select by ID"
    }
  ],
  "teardown": [
    "DROP TABLE users"
  ]
}
```

SQL format:
```sql
-- setup
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');

-- queries
-- name: select_all
SELECT * FROM users;

-- name: select_by_id
SELECT * FROM users WHERE id = 1;

-- teardown
DROP TABLE users;
```

---

## config — Manage Configuration

View, validate, or generate DBMS configuration files.

### Usage
```bash
rosetta config <action> [options]
```

### Actions

| Action | Description |
|--------|-------------|
| `init` | Generate a sample `dbms_config.json` |
| `show` | Display current config details |
| `validate` | Validate JSON structure and test connectivity |

### Examples

```bash
# Generate sample config
rosetta config init

# Generate to custom path
rosetta config init --output my_config.json

# Show current config
rosetta config show

# Validate config
rosetta config validate

# Use custom config file
rosetta config show -c /path/to/config.json
```

---

## result — Browse Historical Results

List, inspect, and navigate past MTR and benchmark runs.

### Usage
```bash
rosetta result list [options]
rosetta result show [RUN_ID] [options]
```

### `result list` Options

| Argument | Default | Description |
|----------|---------|-------------|
| `-n / --limit` | `20` | Rows per page |
| `-p / --page` | `1` | Page number |
| `--type` | `all` | Filter: `all`, `mtr`, `bench` |
| `-o / --output-dir` | `results` | Results directory |

### `result show` Options

| Argument | Default | Description |
|----------|---------|-------------|
| `run_id` | latest | Run ID or prefix (optional) |
| `-o / --output-dir` | `results` | Results directory |

### Examples

```bash
# List all results
rosetta result list

# Pagination
rosetta result list -n 10 -p 2

# Filter by type
rosetta result list --type mtr
rosetta result list --type bench

# Show latest run details
rosetta result show

# Show specific run
rosetta result show bench_json_mv_select_20260331

# JSON output
rosetta result show -j
```

---

## interactive / i / repl — Interactive REPL

Launch an interactive session for ad-hoc SQL execution, MTR tests, and benchmarks.

### Usage
```bash
rosetta i [options]
rosetta interactive [options]
rosetta repl [options]
```

### Options

| Argument | Default | Description |
|----------|---------|-------------|
| `--dbms` | all enabled | DBMS targets (comma-separated) |
| `-d / --database` | `cross_dbms_test_db` | Test database name |
| `-o / --output-dir` | `results` | Report output directory |
| `-s / --serve` | `False` | Start HTTP server for reports |
| `-p / --port` | `19527` | HTTP server port |

### Examples

```bash
# Launch interactive mode
rosetta i

# With specific databases
rosetta i --dbms mysql,tdsql

# With report server
rosetta i --serve
```

### Interactive Commands

Once in the REPL, you can use:
- SQL statements (ending with `;`)
- `.mtr <test_file>` - Run MTR test
- `.bench <bench_file>` - Run benchmark
- `.status` - Check DB status
- `.help` - Show help
- `.quit` or `.exit` - Exit REPL

---

## Best Practices

### 1. Use JSON Output for Automation

```bash
# JSON output is machine-readable
rosetta status -j | jq '.databases[] | select(.status=="failed")'
```

### 2. Version Control Your Config

```bash
# Keep config in version control
git add dbms_config.json
git commit -m "Add production database config"
```

### 3. Use Baselines for MTR Tests

```bash
# Always specify a baseline for consistent diff comparison
rosetta mtr --dbms mysql,tdsql,tidb --baseline mysql -t test.test
```

### 4. Warmup Before Benchmarks

```bash
# Warmup helps get stable performance numbers
rosetta bench --dbms mysql,tdsql --file bench.json \
  --iterations 10 --warmup 3
```

### 5. Reuse Tables for Multiple Benchmarks

```bash
# First run with setup
rosetta bench --dbms mysql --file bench1.json

# Subsequent runs skip setup
rosetta bench --dbms mysql --file bench2.json --skip-setup

# Clean up when done
rosetta bench --dbms mysql --file bench_final.json
```

### 6. Use Query Timeout for Slow Queries

```bash
# Prevent long-running queries from blocking benchmark
rosetta bench --dbms mysql --file bench.json --query-timeout 10
```

### 7. Parallel DBMS Execution

```bash
# Run benchmarks on multiple DBMS in parallel (default)
rosetta bench --dbms mysql,tdsql,tidb --file bench.json

# Or sequentially if needed
rosetta bench --dbms mysql,tdsql,tidb --file bench.json --no-parallel-dbms
```

### 8. Review Historical Results

```bash
# Always review past results before making changes
rosetta result list --type mtr
rosetta result show
```
