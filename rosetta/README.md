# Rosetta

Cross-DBMS SQL behavioral consistency verification tool.

Rosetta parses MySQL MTR-style `.test` files, executes the SQL statements against multiple database systems (TDSQL, MySQL, TiDB, OceanBase, etc.), compares execution results, and generates visual diff reports.

## Requirements

- Python >= 3.8
- PyMySQL >= 1.0

## Installation

```bash
cd .doc
pip install -e .
```

After installation, the `rosetta` command is available globally.

## Quick Start

```bash
# 1. Generate a sample config file
rosetta --gen-config dbms_config.json

# 2. Edit dbms_config.json with your DBMS connection info

# 3. Run a test
rosetta --test path/to/test.test --config dbms_config.json --dbms tdsql,mysql
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
| `--config, -c` | `.doc/dbms_config.json` | Path to DBMS config JSON file |
| `--dbms` | *(all enabled)* | DBMS to compare, comma-separated (e.g. `tdsql,mysql,tidb`) |
| `--baseline, -b` | `tdsql` | Baseline DBMS name for comparison |
| `--output-dir, -o` | `.doc/cross_dbms_results` | Output directory for reports |
| `--format, -f` | `all` | Output format: `text`, `html`, or `all` |
| `--database, -d` | `cross_dbms_test_db` | Test database name |
| `--skip-explain` | `True` | Skip EXPLAIN statements |
| `--skip-analyze` | `False` | Skip ANALYZE TABLE statements |
| `--skip-show-create` | `False` | Skip SHOW CREATE TABLE statements |
| `--parse-only` | `False` | Only parse `.test` file and print statements (debug) |
| `--diff-only` | `False` | Re-generate reports from existing `.result` files without executing |
| `--gen-config` | — | Generate a sample config file at the given path and exit |
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

## Project Structure

```
.doc/
├── pyproject.toml         # Package metadata and console_scripts entry
├── setup.py               # Fallback install entry
└── rosetta/               # Python package
    ├── __init__.py        # Package definition
    ├── __main__.py        # python -m rosetta entry point
    ├── models.py          # Data models (Statement, StmtResult, DBMSConfig, CompareResult)
    ├── config.py          # Config loading, validation, sample generation
    ├── parser.py          # MTR .test file parser
    ├── executor.py        # DB connection management and SQL execution
    ├── comparator.py      # Result normalization and diff comparison
    ├── reporter/
    │   ├── __init__.py
    │   ├── text.py        # Text report generator
    │   └── html.py        # HTML visual report generator
    └── cli.py             # CLI entry point and orchestration
```
