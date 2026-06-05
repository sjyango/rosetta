# Troubleshooting Guide

Common issues and solutions when using rosetta.

---

## Installation Issues

### Python Version Too Old

**Error:**
```
Python 3.8+ is required. Current version: 3.6.9
```

**Solution:**
```bash
# Check Python version
python3 --version

# Install Python 3.8+ (Ubuntu/Debian)
sudo apt update
sudo apt install python3.9 python3.9-venv python3.9-dev

# Or use pyenv
curl https://pyenv.run | bash
pyenv install 3.9.0
pyenv global 3.9.0
```

---

### pip Not Found

**Error:**
```
Command 'pip' not found
```

**Solution:**
```bash
# Install pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# Or use python -m pip
python3 -m pip install pymysql rich prompt_toolkit
```

---

### GitHub API Rate Limit

**Error:**
```
GitHub API rate limit exceeded. Please set GITHUB_TOKEN environment variable.
```

**Solution:**
```bash
# Create GitHub Personal Access Token
# https://github.com/settings/tokens

# Set environment variable
export GITHUB_TOKEN="your_token_here"

# Or add to ~/.bashrc or ~/.zshrc
echo 'export GITHUB_TOKEN="your_token_here"' >> ~/.bashrc
source ~/.bashrc

# Re-run installation
python install_rosetta.py
```

**Alternative:** Use source installation:
```bash
python install_rosetta.py --source
```

---

### Download Failed

**Error:**
```
Failed to download https://github.com/.../rosetta-v1.0.0.pyz
```

**Solutions:**

1. **Check network connection:**
```bash
ping github.com
curl -I https://github.com
```

2. **Use proxy:**
```bash
export HTTP_PROXY="http://proxy.example.com:8080"
export HTTPS_PROXY="http://proxy.example.com:8080"
```

3. **Use source installation:**
```bash
python install_rosetta.py --source
```

---

### SHA256 Verification Failed

**Error:**
```
SHA256 mismatch!
  Expected: abc123...
  Actual:   def456...
```

**Solution:**
```bash
# Remove cached file and re-download
rm -rf ~/.rosetta/cache/*
python install_rosetta.py --force
```

---

## Configuration Issues

### Configuration File Not Found

**Error:**
```
Configuration file not found: rosetta_config.json
```

**Solution:**
```bash
# Generate sample config
rosetta config init

# Or specify config path
rosetta status -c /path/to/config.json
```

---

### Invalid JSON Syntax

**Error:**
```
JSON decode error: Expecting ',' delimiter
```

**Solution:**
```bash
# Validate JSON syntax
python3 -m json.tool rosetta_config.json

# Common mistakes:
# - Missing comma after field
# - Trailing comma in last element
# - Unquoted strings
# - Single quotes instead of double quotes
```

---

### Connection Refused

**Error:**
```
Connection failed: Connection refused
```

**Solutions:**

1. **Check database server is running:**
```bash
# MySQL
systemctl status mysql
# or
systemctl status mysqld

# Check port
netstat -tlnp | grep 3306
```

2. **Verify host and port:**
```bash
# Test connectivity
telnet <host> <port>
# or
nc -zv <host> <port>
```

3. **Check firewall:**
```bash
# Linux
sudo iptables -L -n | grep 3306
sudo firewall-cmd --list-ports

# macOS
sudo lsof -i :3306
```

---

### Authentication Failed

**Error:**
```
Access denied for user 'root'@'localhost' (using password: YES)
```

**Solutions:**

1. **Check credentials:**
```bash
# Test with mysql client
mysql -h <host> -P <port> -u <user> -p
```

2. **Verify password in config:**
```json
{
  "password": "your_password_here"
}
```

3. **Use environment variable:**
```json
{
  "password": "${MYSQL_PASSWORD}"
}
```
```bash
export MYSQL_PASSWORD="your_password"
```

---

### Unknown Database

**Error:**
```
Unknown database 'mydb'
```

**Solution:**
```bash
# Create database
mysql -u root -p -e "CREATE DATABASE mydb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Or connect without specifying database
rosetta exec --dbms mysql --sql "SHOW DATABASES"
```

---

## MTR Test Issues

### Test File Not Found

**Error:**
```
Test file not found: test.test
```

**Solution:**
```bash
# Use absolute path
rosetta mtr --dbms mysql,tdsql -t /absolute/path/to/test.test

# Or check current directory
pwd
ls -la test.test
```

---

### Parse Error in Test File

**Error:**
```
Parse error in test file: line 42
```

**Solution:**
```bash
# Validate test file syntax
rosetta mtr --dbms mysql --parse-only -t test.test

# Common mistakes:
# - Unclosed string literals
# - Missing semicolons
# - Invalid SQL syntax
```

---

### Database Already Exists

**Error:**
```
Database 'rosetta_mtr_test' already exists
```

**Solution:**
```bash
# Use different database name
rosetta mtr --dbms mysql,tdsql -t test.test --database my_test_db

# Or drop existing database
mysql -u root -p -e "DROP DATABASE IF EXISTS rosetta_mtr_test;"
```

---

### Test Timeout

**Error:**
```
Query timeout: SELECT SLEEP(100)
```

**Solution:**
```bash
# This is expected behavior for long-running queries
# Add to skip_patterns in config if needed:
{
  "skip_patterns": ["SLEEP\\(\\d+\\)"]
}
```

---

## Benchmark Issues

### Benchmark File Not Found

**Error:**
```
Benchmark file not found: bench.json
```

**Solution:**
```bash
# Use absolute path
rosetta bench --dbms mysql --file /path/to/bench.json

# Or check file exists
ls -la bench.json
```

---

### Invalid Benchmark JSON

**Error:**
```
Invalid benchmark file: missing 'queries' field
```

**Solution:**
```bash
# Validate JSON syntax
python3 -m json.tool bench.json

# Check required fields
{
  "setup": [...],      # optional
  "queries": [...]     # required
  "teardown": [...]    # optional
}
```

---

### Query Syntax Error

**Error:**
```
SQL syntax error near 'SELCT * FROM users'
```

**Solution:**
```bash
# Fix SQL syntax in benchmark file
# Test query manually first
mysql -u root -p -e "SELECT * FROM users;"
```

---

### Out of Memory

**Error:**
```
MemoryError or Killed
```

**Solutions:**

1. **Reduce concurrency:**
```bash
rosetta bench --dbms mysql --file bench.json \
  --mode CONCURRENT --concurrency 4
```

2. **Use smaller dataset:**
```json
{
  "setup": [
    "CREATE TABLE users (id INT PRIMARY KEY)",
    "INSERT INTO users SELECT seq FROM seq_1_to_1000"
  ]
}
```

3. **Increase system resources:**
```bash
# Check memory
free -h

# Add swap (Linux)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

## Performance Issues

### Slow Connections

**Problem:** Connections take a long time to establish

**Solutions:**

1. **Check DNS resolution:**
```bash
# Test DNS
nslookup your-db-host.com

# Use IP address instead
{
  "host": "192.168.1.100",
  "port": 3306
}
```

2. **Disable reverse DNS lookup:**
```sql
-- MySQL config
[mysqld]
skip-name-resolve
```

3. **Increase connection timeout:**
```bash
rosetta status --timeout 20
```

---

### Slow Test Execution

**Problem:** MTR tests run very slowly

**Solutions:**

1. **Use fewer databases:**
```bash
# Test one at a time
rosetta mtr --dbms mysql -t test.test
```

2. **Skip expensive operations:**
```bash
rosetta mtr --dbms mysql,tdsql -t test.test \
  --skip-explain \
  --skip-analyze
```

3. **Optimize database:**
```sql
-- Increase buffer pool
SET GLOBAL innodb_buffer_pool_size = 1073741824;

-- Increase log buffer
SET GLOBAL innodb_log_buffer_size = 16777216;
```

---

### Slow Benchmark

**Problem:** Benchmarks take too long

**Solutions:**

1. **Use serial mode:**
```bash
rosetta bench --dbms mysql --file bench.json --mode SERIAL --iterations 1
```

2. **Run specific queries:**
```bash
rosetta bench --dbms mysql --file bench.json --bench-filter "query1,query2"
```

3. **Skip setup:**
```bash
# First run with setup
rosetta bench --dbms mysql --file bench.json

# Subsequent runs skip setup
rosetta bench --dbms mysql --file bench.json --skip-setup
```

---

## Output Issues

### HTML Report Not Opening

**Problem:** Browser doesn't open HTML report

**Solution:**
```bash
# Manually open report
open results/test_name.html        # macOS
xdg-open results/test_name.html    # Linux
start results/test_name.html       # Windows

# Or use rosetta's built-in server
rosetta mtr --dbms mysql,tdsql -t test.test --serve
```

---

### Missing Results

**Problem:** Can't find test results

**Solution:**
```bash
# List all results
rosetta result list

# Check output directory
ls -la results/

# Specify output directory
rosetta mtr --dbms mysql,tdsql -t test.test -o my_results
```

---

### Permission Denied

**Error:**
```
Permission denied: results/test.report.txt
```

**Solution:**
```bash
# Fix permissions
chmod -R u+rw results/

# Or use different output directory
rosetta mtr --dbms mysql,tdsql -t test.test -o ~/my_results
```

---

## Dependency Issues

### Module Not Found

**Error:**
```
ModuleNotFoundError: No module named 'pymysql'
```

**Solution:**
```bash
# Install missing module
pip install pymysql

# Or install all dependencies
pip install pymysql rich prompt_toolkit
```

---

### Version Conflict

**Error:**
```
ERROR: pip's dependency resolver does not currently take into account all the packages...
```

**Solution:**
```bash
# Use virtual environment
python3 -m venv venv
source venv/bin/activate
pip install pymysql rich prompt_toolkit

# Or force reinstall
pip install --force-reinstall pymysql rich prompt_toolkit
```

---

## Getting Help

### Check Rosetta Version

```bash
rosetta --version
rosetta -v
```

### Enable Verbose Logging

```bash
# Enable debug output
rosetta status -v
rosetta mtr --dbms mysql,tdsql -t test.test -v
```

### Check Logs

```bash
# Rosetta doesn't write log files by default
# Use -v flag to see verbose output on console
rosetta mtr --dbms mysql,tdsql -t test.test -v 2>&1 | tee rosetta.log
```

### Report Issues

If you encounter issues not covered in this guide:

1. **Collect diagnostic information:**
```bash
# System info
python3 --version
pip list | grep -E "pymysql|rich|prompt"

# Rosetta info
rosetta --version
rosetta config validate -v

# Test case
rosetta mtr --dbms mysql,tdsql -t test.test -v
```

2. **Create issue on GitHub:**
   - https://github.com/sjyango/rosetta/issues
   - Include diagnostic information
   - Include minimal reproduction case

3. **Check existing issues:**
   - Search for similar problems
   - Check closed issues for solutions
