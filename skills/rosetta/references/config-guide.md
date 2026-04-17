# Configuration Guide

Detailed guide for configuring rosetta to connect to your databases.

## Configuration File

Rosetta uses a JSON configuration file (default: `rosetta_config.json`) to define database connections.

### Basic Structure

```json
{
  "databases": [
    {
      "name": "mysql",
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

---

## Database Configuration Fields

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique identifier for this database (used in `--dbms` argument) |
| `host` | string | Database host address |
| `port` | integer | Database port number |
| `user` | string | Database username |
| `password` | string | Database password (can be empty string) |
| `driver` | string | Database driver: `pymysql` (default) or `mysql-connector` |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Whether this database is active |
| `skip_patterns` | array | `[]` | Regex patterns for queries to skip |
| `init_sql` | array | `[]` | SQL statements to run on connection |
| `skip_explain` | boolean | `false` | Skip EXPLAIN statements in MTR |
| `skip_analyze` | boolean | `false` | Skip ANALYZE TABLE statements in MTR |
| `skip_show_create` | boolean | `false` | Skip SHOW CREATE TABLE in MTR |
| `charset` | string | `utf8mb4` | Character set for connection |
| `ssl` | object | `null` | SSL configuration |
| `connect_timeout` | integer | `10` | Connection timeout in seconds |
| `read_timeout` | integer | `30` | Read timeout in seconds |
| `write_timeout` | integer | `30` | Write timeout in seconds |

---

## Driver Options

### pymysql (Default)

Most common driver, works with MySQL, TDSQL, TiDB, OceanBase.

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "pymysql"
}
```

**Installation:**
```bash
pip install pymysql>=1.0
```

### mysql-connector

Official MySQL connector, better SSL support.

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "mysql-connector"
}
```

**Installation:**
```bash
pip install mysql-connector-python>=8.0
```

---

## Database-Specific Configurations

### MySQL

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "enabled": true,
  "init_sql": [
    "SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
  ]
}
```

### TDSQL

```json
{
  "name": "tdsql",
  "host": "127.0.0.1",
  "port": 4000,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "enabled": true,
  "init_sql": []
}
```

### TiDB

```json
{
  "name": "tidb",
  "host": "127.0.0.1",
  "port": 4000,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "enabled": true,
  "skip_explain": true,
  "skip_analyze": true,
  "skip_show_create": true
}
```

**Note:** TiDB doesn't fully support EXPLAIN, ANALYZE TABLE, and SHOW CREATE TABLE, so these are skipped by default.

### OceanBase

```json
{
  "name": "oceanbase",
  "host": "127.0.0.1",
  "port": 2881,
  "user": "root@mysql",
  "password": "",
  "driver": "pymysql",
  "enabled": true,
  "skip_explain": true,
  "skip_analyze": true,
  "skip_show_create": true
}
```

**Note:** OceanBase uses `user@tenant` format for username.

---

## Advanced Features

### Init SQL

Execute SQL statements when establishing connection:

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "init_sql": [
    "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
    "SET SESSION time_zone = '+00:00'",
    "SET NAMES utf8mb4"
  ]
}
```

### Skip Patterns

Skip specific queries using regex patterns:

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "skip_patterns": [
    "SHOW SLAVE STATUS",
    "SHOW MASTER STATUS",
    "SHOW BINARY LOGS"
  ]
}
```

### SSL Configuration

Enable SSL for secure connections:

```json
{
  "name": "mysql",
  "host": "prod-db.example.com",
  "port": 3306,
  "user": "readonly",
  "password": "secret",
  "driver": "pymysql",
  "ssl": {
    "ca": "/path/to/ca.pem",
    "cert": "/path/to/client-cert.pem",
    "key": "/path/to/client-key.pem",
    "check_hostname": true
  }
}
```

### Connection Pooling

Configure connection pool settings:

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "",
  "driver": "pymysql",
  "pool_size": 5,
  "max_overflow": 10,
  "pool_timeout": 30
}
```

---

## Multiple Environments

### Development vs Production

Create separate config files for different environments:

```bash
# Development
rosetta status -c rosetta_config.dev.json

# Production
rosetta status -c rosetta_config.prod.json
```

### Example: Multi-Environment Setup

**rosetta_config.dev.json:**
```json
{
  "databases": [
    {
      "name": "mysql-dev",
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "",
      "driver": "pymysql"
    },
    {
      "name": "tdsql-dev",
      "host": "localhost",
      "port": 4000,
      "user": "root",
      "password": "",
      "driver": "pymysql"
    }
  ]
}
```

**rosetta_config.prod.json:**
```json
{
  "databases": [
    {
      "name": "mysql-prod",
      "host": "prod-mysql.example.com",
      "port": 3306,
      "user": "readonly",
      "password": "${MYSQL_PASSWORD}",
      "driver": "pymysql",
      "ssl": {
        "ca": "/etc/ssl/certs/ca.pem"
      }
    },
    {
      "name": "tdsql-prod",
      "host": "prod-tdsql.example.com",
      "port": 4000,
      "user": "readonly",
      "password": "${TDSQL_PASSWORD}",
      "driver": "pymysql"
    }
  ]
}
```

---

## Environment Variables

### Password from Environment

Use environment variables for sensitive data:

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "${MYSQL_PASSWORD}",
  "driver": "pymysql"
}
```

Set environment variable:
```bash
export MYSQL_PASSWORD="your_password"
rosetta status
```

### Common Environment Variables

```bash
# Database credentials
export MYSQL_HOST="127.0.0.1"
export MYSQL_PORT="3306"
export MYSQL_USER="root"
export MYSQL_PASSWORD="secret"

# Rosetta configuration
export ROSETTA_CONFIG="/path/to/config.json"
export ROSETTA_OUTPUT_DIR="/path/to/results"
```

---

## Validation

### Validate Configuration

```bash
# Check JSON syntax and test connections
rosetta config validate

# Output:
# ✓ JSON syntax is valid
# ✓ mysql: Connection successful (MySQL 8.0.26)
# ✓ tdsql: Connection successful (TDSQL 10.3.9)
# ✗ tidb: Connection failed - Connection refused
```

### Common Validation Errors

**1. JSON Syntax Error**
```
✗ JSON syntax error: Expecting ',' delimiter: line 15 column 5 (char 342)
```
→ Fix JSON syntax, use a JSON validator.

**2. Connection Refused**
```
✗ mysql: Connection failed - Connection refused
```
→ Check host, port, and database server status.

**3. Authentication Failed**
```
✗ mysql: Connection failed - Access denied for user 'root'@'localhost'
```
→ Check username and password.

**4. Unknown Database**
```
✗ mysql: Unknown database 'mydb'
```
→ Database doesn't exist or wrong database name.

---

## Best Practices

### 1. Use Descriptive Names

```json
{
  "name": "mysql-prod-master",
  "host": "prod-master.example.com",
  "port": 3306,
  ...
}
```

### 2. Disable Unused Databases

```json
{
  "name": "mysql-backup",
  "host": "backup.example.com",
  "port": 3306,
  "user": "readonly",
  "password": "",
  "driver": "pymysql",
  "enabled": false
}
```

### 3. Use Read-Only Users for Testing

```json
{
  "name": "mysql-prod",
  "host": "prod.example.com",
  "port": 3306,
  "user": "readonly_user",
  "password": "${READONLY_PASSWORD}",
  "driver": "pymysql"
}
```

### 4. Version Control Config (Without Passwords)

```json
{
  "name": "mysql",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "${MYSQL_PASSWORD}",
  "driver": "pymysql"
}
```

```bash
# .gitignore
rosetta_config.json
rosetta_config.*.json

# Use template
git add rosetta_config.example.json
```

### 5. Test Configuration Before Running Tests

```bash
# Always validate before running MTR tests
rosetta config validate && rosetta mtr --dbms mysql,tdsql -t test.test
```

### 6. Use Comments (JSON doesn't support comments natively)

Use a preprocessor or separate documentation:

```json
{
  "databases": [
    {
      "name": "mysql",
      "_comment": "Production MySQL master - updated 2026-04-01",
      "host": "prod-mysql.example.com",
      "port": 3306,
      "user": "readonly",
      "password": "${MYSQL_PASSWORD}",
      "driver": "pymysql"
    }
  ]
}
```

---

## Troubleshooting

### Connection Issues

**Problem:** Connection timeout
```
✗ mysql: Connection timed out
```

**Solutions:**
- Check firewall rules
- Increase timeout: `rosetta status --timeout 20`
- Verify host and port are correct

### Driver Issues

**Problem:** Driver not found
```
ModuleNotFoundError: No module named 'pymysql'
```

**Solution:**
```bash
pip install pymysql>=1.0
```

### SSL Issues

**Problem:** SSL certificate verification failed
```
SSL: CERTIFICATE_VERIFY_FAILED
```

**Solution:**
```json
{
  "ssl": {
    "ca": "/path/to/ca.pem",
    "check_hostname": false
  }
}
```

### Character Set Issues

**Problem:** UnicodeEncodeError
```
UnicodeEncodeError: 'utf-8' codec can't encode character...
```

**Solution:**
```json
{
  "charset": "utf8mb4",
  "init_sql": ["SET NAMES utf8mb4"]
}
```
