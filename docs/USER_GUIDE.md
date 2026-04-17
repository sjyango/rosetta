# Rosetta 使用指南

> Cross-DBMS SQL Behavioral Consistency Verification Tool
>
> 跨数据库 SQL 行为一致性验证工具

Rosetta 用于在多个数据库（MySQL、TDSQL、TiDB、OceanBase 等）上执行相同的 MTR 风格测试文件，自动对比执行结果差异，生成可视化报告。

---

## 目录

- [快速开始](#快速开始)
- [安装方式](#安装方式)
- [配置文件](#配置文件)
- [编写测试文件](#编写测试文件)
- [运行测试](#运行测试)
- [交互模式](#交互模式)
- [查看报告](#查看报告)
- [白名单与 Bug 标记](#白名单与-bug-标记)
- [命令行参数](#命令行参数)
- [高级用法](#高级用法)
- [打包与分发](#打包与分发)
- [FAQ](#faq)

---

## 快速开始

```bash
# 1. 安装 Python 依赖（仅首次）
pip install pymysql rich

# 2. 生成配置文件
python3 rosetta.pyz --gen-config rosetta_config.json

# 3. 编辑配置，填入你的数据库连接信息
vim rosetta_config.json

# 4. 运行测试
python3 rosetta.pyz --test path/to/your_test.test --dbms tdsql,mysql

# 5. 查看 HTML 报告（自动启动 HTTP 服务）
python3 rosetta.pyz --test path/to/your_test.test --dbms tdsql,mysql --serve
```

---

## 安装方式

### 前置条件

- Python 3.8+
- pip

### 方式一：.pyz 单文件（推荐）

`rosetta.pyz` 是一个打包好的 Python zip 应用，只有一个文件，拷过去就能用：

```bash
# 1. 安装运行时依赖
pip install pymysql "rich>=13.0"

# 2. 直接运行
python3 rosetta.pyz --help

# 也可以加执行权限后直接运行（Linux/macOS）
chmod +x rosetta.pyz
./rosetta.pyz --help
```

**分发给他人只需提供 `rosetta.pyz` 一个文件**，对方安装好 Python 依赖即可使用。

### 方式二：pip 安装（开发者）

如果你有完整的源码目录，可以用 pip 安装为系统命令：

```bash
cd rosetta/
pip install -e .
rosetta --help
```

---

## 配置文件

Rosetta 通过 JSON 配置文件管理数据库连接。默认读取当前目录下的 `rosetta_config.json`。

### 生成示例配置

```bash
python3 rosetta.pyz --gen-config rosetta_config.json
```

### 配置格式

```json
{
  "databases": [
    {
      "name": "tdsql",
      "host": "127.0.0.1",
      "port": 10886,
      "user": "test",
      "password": "test123",
      "driver": "pymysql",
      "skip_patterns": [],
      "init_sql": [],
      "skip_explain": false,
      "skip_analyze": false,
      "skip_show_create": false,
      "enabled": true,
      "restart_cmd": ""
    },
    {
      "name": "mysql",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "root",
      "password": "",
      "driver": "pymysql",
      "init_sql": ["SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"],
      "enabled": true
    }
  ]
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `name` | string | 数据库标识名，用于命令行 `--dbms` 和报告展示 |
| `host` | string | 数据库地址 |
| `port` | int | 端口号 |
| `user` | string | 用户名 |
| `password` | string | 密码 |
| `driver` | string | 连接驱动，默认 `pymysql` |
| `init_sql` | list | 连接后执行的初始化 SQL |
| `skip_explain` | bool | 跳过 EXPLAIN 语句 |
| `skip_analyze` | bool | 跳过 ANALYZE TABLE 语句 |
| `skip_show_create` | bool | 跳过 SHOW CREATE TABLE 语句 |
| `enabled` | bool | 未指定 `--dbms` 时，是否默认启用 |
| `restart_cmd` | string | 数据库重启命令（异常恢复用） |

---

## 编写测试文件

Rosetta 使用 MTR（MySQL Test Runner）风格的 `.test` 文件。

### 基本语法

```sql
# 注释（会作为分隔标记出现在结果中）

--echo # Section 1: Basic Tests

CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50));
INSERT INTO t1 VALUES (1, 'foo'), (2, 'bar');
SELECT * FROM t1 ORDER BY id;

# 多行 SQL
SELECT
    id, name
FROM t1
WHERE id > 0
ORDER BY id;

# 期望报错
--error ER_DUP_ENTRY
INSERT INTO t1 VALUES (1, 'duplicate');

# 结果排序后比较（用于无序结果集）
--sorted_result
SELECT * FROM t1;

DROP TABLE t1;
```

### 支持的指令

| 指令 | 说明 |
|---|---|
| `# comment` | 注释，作为分隔标记出现在结果文件中 |
| `--echo text` | 在结果中输出标记文本 |
| `--error code` | 标记下一条 SQL 预期报错 |
| `--sorted_result` | 下一条 SQL 结果排序后再比较 |
| `--delimiter` | 更改语句分隔符 |
| `--source file` | 包含另一个测试文件 |

### 最佳实践

1. 用 `--echo` 或 `#` 注释将测试分成逻辑段落，便于在报告中定位
2. 对 SELECT 加 `ORDER BY` 确保结果顺序一致
3. 测试结束时 DROP 创建的表（Rosetta 会自动创建独立的测试数据库 `cross_dbms_test_db`）
4. 使用 `--error` 标记预期报错，报错信息也会纳入结果比较

---

## 运行测试

### 基本运行

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql
```

### 指定 baseline

```bash
# 以 tdsql 为基准对比
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql --baseline tdsql
```

默认 baseline 为 `tdsql`。指定后，报告只展示 baseline vs 其他数据库的对比。

### 运行并查看报告

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql --serve
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql --serve --port 8080
```

### 仅重新生成报告（不执行 SQL）

```bash
python3 rosetta.pyz --test my_test.test --diff-only
```

### 仅解析测试文件（调试用）

```bash
python3 rosetta.pyz --test my_test.test --parse-only
```

---

## 交互模式

交互模式提供一个 REPL 会话，可以反复提交 `.test` 文件路径执行测试，无需重启程序。

### 启动交互模式

```bash
python3 rosetta.pyz --interactive --config rosetta_config.json --dbms tdsql,mysql --serve
```

启动后会显示 `rosetta ▶` 提示符，输入 `.test` 文件路径即可执行测试。

### 交互模式功能

- **Tab 自动补全**：输入路径时按 Tab 自动补全 `.test` 文件和目录
- **内置 HTTP 服务**：配合 `--serve` 自动启动 Web 服务，实时查看报告
- **历史记录页面**：每次运行后自动更新 `index.html` 汇总页面
- **白名单 / Bug 标记管理**：通过 Web UI 或 REST API 管理

### 交互模式命令

| 命令 | 说明 |
|---|---|
| `<路径>.test` | 执行指定的测试文件 |
| `quit` / `exit` | 退出交互模式 |
| `Ctrl+C` | 中断当前执行 |
| `Ctrl+D` | 退出 |

---

## 查看报告

每次运行在 `results/` 下生成带时间戳的子目录：

```
results/
├── my_test_20260309_172119/
│   ├── my_test.tdsql.result     # tdsql 执行结果
│   ├── my_test.mysql.result     # mysql 执行结果
│   ├── my_test.report.txt       # 文本报告
│   ├── my_test.diff             # diff 文件
│   └── my_test.html             # HTML 交互式报告
├── latest -> my_test_20260309_172119
├── index.html                   # 历史运行汇总页面
├── whitelist.json               # 白名单数据（所有运行共享）
├── whitelist.html               # 白名单管理页面
├── buglist.json                 # Bug 标记数据（所有运行共享）
└── buglist.html                 # Bug 标记管理页面
```

### HTML 报告

- **Summary 表格**：每对数据库的匹配数、差异数、白名单数、Bug 标记数、通过率
- **Diff 详情**：点击展开差异 block，左右对比两个数据库输出
- **白名单/Bug 操作按钮**：每个 diff block 可直接加白或标记 Bug
- **上下文导航**：每个 diff 上方展示前后相邻 SQL，快速定位
- **行号标识**：每条 SQL 带 `[Lxxx]` 前缀（对应 .test 文件行号），相同 SQL 也可唯一区分

### 文本报告

包含 unified diff 格式输出和上下文信息，适合终端查看和 CI 集成。Summary 表格也会展示白名单和 Bug 标记列（如有）。

### 历史页面

`--serve` 启动 HTTP 服务后访问 `http://localhost:19527/index.html` 查看所有历史运行，支持按测试名和 DBMS 过滤。历史页面同时提供白名单管理页面和 Bug 列表管理页面的入口。

---

## 白名单与 Bug 标记

Rosetta 支持对单个 diff block 进行**白名单**和 **Bug 标记**管理，帮助跨测试运行追踪已知差异。

### 白名单（Whitelist）

白名单中的 diff **不计入失败数**——它们不再导致测试被判定为 FAIL。适用于已知的可接受差异（例如，不同数据库的特性差异但行为正确）。

- 每个 diff 通过 **MD5 指纹**识别，基于标准化的 SQL 语句和两端输出计算
- 白名单条目持久化在输出目录下的 `whitelist.json` 中
- 白名单 diff 在 HTML 报告中以降低透明度和黄色 "Whitelisted" 徽章显示

### Bug 标记（Buglist）

Bug 标记是**仅供参考的标注**——标记的 diff 仍计入失败率，但会在视觉上区分显示，便于追踪已知 bug。

- Bug 条目持久化在输出目录下的 `buglist.json` 中
- Bug 标记的 diff 在 HTML 报告中以红色左边框和 "Bug" 徽章显示

### 通过 HTML 报告管理

在每个测试运行的 HTML 报告中，每个 diff block 都有操作按钮：

- **加白** —— 将 diff 加入白名单，再次点击可移除
- **标记Bug** —— 标记为已知 Bug，再次点击可移除

更改立即生效并持久化到 JSON 文件。从历史页面重新打开报告时，白名单/Bug 标记状态会通过 API 自动同步。

### 通过 REST API 管理

启动 `--serve` 后，以下 API 端点可用：

| 端点 | 方法 | 说明 | 请求体 |
|---|---|---|---|
| `/api/whitelist/list` | POST | 列出所有白名单条目 | `{}` |
| `/api/whitelist/add` | POST | 添加白名单条目 | `{"fingerprint":"...", "stmt":"...", "dbms_a":"...", "dbms_b":"...", "block":0, "reason":""}` |
| `/api/whitelist/remove` | POST | 移除白名单条目 | `{"fingerprint":"..."}` |
| `/api/whitelist/clear` | POST | 清空白名单 | `{}` |
| `/api/buglist/list` | POST | 列出所有 Bug 条目 | `{}` |
| `/api/buglist/add` | POST | 添加 Bug 条目 | `{"fingerprint":"...", "stmt":"...", "dbms_a":"...", "dbms_b":"...", "block":0, "reason":""}` |
| `/api/buglist/remove` | POST | 移除 Bug 条目 | `{"fingerprint":"..."}` |
| `/api/buglist/clear` | POST | 清空 Bug 列表 | `{}` |

### 管理页面

历史首页提供两个独立管理页面的链接：

- **whitelist.html** —— 查看、搜索、删除白名单条目
- **buglist.html** —— 查看、搜索、删除 Bug 标记条目

---

## 命令行参数

| 参数 | 短写 | 默认值 | 说明 |
|---|---|---|---|
| `--test` | `-t` | （必填） | .test 测试文件路径 |
| `--config` | `-c` | `rosetta_config.json` | 数据库配置文件路径 |
| `--dbms` | | 按 enabled 字段 | 要测试的数据库，逗号分隔 |
| `--baseline` | `-b` | `tdsql` | 基准数据库 |
| `--output-dir` | `-o` | `results` | 输出目录 |
| `--format` | `-f` | `all` | 输出格式：`text` / `html` / `all` |
| `--database` | `-d` | `cross_dbms_test_db` | 测试用数据库名 |
| `--skip-explain` | | `true` | 跳过 EXPLAIN 语句 |
| `--skip-analyze` | | `false` | 跳过 ANALYZE TABLE |
| `--skip-show-create` | | `false` | 跳过 SHOW CREATE TABLE |
| `--parse-only` | | | 仅解析 .test 文件 |
| `--diff-only` | | | 仅从 .result 重新生成报告 |
| `--gen-config` | | | 生成示例配置并退出 |
| `--serve` | `-s` | | 运行后启动 HTTP 服务 |
| `--port` | `-p` | `19527` | HTTP 服务端口 |
| `--interactive` | | | 进入交互模式（REPL 会话） |
| `--verbose` | `-v` | | 详细日志 |

---

## 高级用法

### 多数据库同时对比

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql,tidb --baseline tdsql
```

生成 `tdsql_vs_mysql` 和 `tdsql_vs_tidb` 两组对比。

### 无 baseline 全排列对比

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql,tidb --baseline ""
```

生成所有数据库两两组合的对比。

### 自定义测试数据库名

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql --database my_test_db
```

Rosetta 会在每个数据库上 `DROP DATABASE IF EXISTS` → `CREATE DATABASE` → 执行测试 → 清理。

### 只生成文本报告

```bash
python3 rosetta.pyz --test my_test.test --dbms tdsql,mysql --format text
```

---

## 打包与分发

### 构建 .pyz 文件

项目自带一键构建脚本：

```bash
chmod +x build.sh
./build.sh
```

构建完成后在 `dist/` 目录生成 `rosetta.pyz`（约 100KB）。

### 手动构建

也可以手动执行 zipapp 打包：

```bash
# 创建临时构建目录
mkdir -p /tmp/rosetta_build
cp -r rosetta /tmp/rosetta_build/rosetta

# 清理 .pyc 缓存
find /tmp/rosetta_build -name '*.pyc' -delete
find /tmp/rosetta_build -name '__pycache__' -type d -exec rm -rf {} +

# 创建入口文件
cat > /tmp/rosetta_build/__main__.py << 'EOF'
import sys
from rosetta.cli import main
sys.exit(main())
EOF

# 打包
python3 -m zipapp /tmp/rosetta_build -p "/usr/bin/env python3" -o dist/rosetta.pyz

# 清理
rm -rf /tmp/rosetta_build
```

### 分发清单

将以下内容提供给使用者：

| 文件 | 说明 |
|---|---|
| `rosetta.pyz` | 工具本体（必需） |
| `rosetta_config.json` | 数据库配置（可通过 `--gen-config` 生成） |
| `.test` 文件 | 测试用例 |

使用者收到后只需：

```bash
# 1. 安装依赖
pip install pymysql "rich>=13.0"

# 2. 生成配置（如果没有现成的）
python3 rosetta.pyz --gen-config rosetta_config.json

# 3. 编辑配置中的数据库连接信息
vim rosetta_config.json

# 4. 运行测试
python3 rosetta.pyz --test your_test.test --dbms tdsql,mysql
```

---

## FAQ

### Q: 执行时报 "Unknown database" 错误？

Rosetta 会自动创建测试数据库（默认名 `cross_dbms_test_db`）。请确保配置中的数据库用户有 `CREATE DATABASE` 和 `DROP DATABASE` 权限。

### Q: pip install 和 .pyz 方式运行命令有什么不同？

- **pip 安装后**：直接使用 `rosetta --test ...`
- **.pyz 方式**：使用 `python3 rosetta.pyz --test ...`

两种方式功能完全相同，参数也一致。本文档统一使用 `python3 rosetta.pyz` 写法。

### Q: 如何跳过某些数据库不支持的语句？

- 使用 `--error` 标记预期报错，这样报错本身会被纳入比较
- 在配置中设置 `skip_explain`、`skip_analyze`、`skip_show_create` 跳过特定类型语句
- 使用 `init_sql` 在连接时设置特定 session 变量

### Q: 结果中 NULL 和空字符串如何区分？

- NULL 显示为 `NULL`
- 空字符串显示为空（无内容）
- 布尔值 true/false 显示为 `1`/`0`

### Q: 如何定位 diff 中的具体 SQL？

每条 SQL 在结果中都带有 `[Lxxx]` 行号前缀（xxx 为 `.test` 文件中的行号），同时 diff 区域会展示前后上下文 SQL，帮助快速定位。

### Q: 两个数据库的执行是串行还是并行？

**并行执行**。每个数据库使用独立的连接和线程，互不干扰。

### Q: diff-only 模式从哪里读取 result 文件？

从 `results/latest` 软链接指向的目录中读取 `.result` 文件。
