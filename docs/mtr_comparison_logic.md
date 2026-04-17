# MTR 测试比对逻辑

## 整体流程

```
Parse（单次） → Execute（并行，每个DBMS独立） → Compare（两两比对） → Report
```

## 1. Parse 阶段

使用 `rosetta.mtr.MtrParser` 解析 `.test` 文件，产出 AST：

```
MtrParser("case.test")
  → 逐行扫描，识别 --directive / 裸SQL / if-while 块
  → --source 引入外部文件
  → 输出 MtrTestFile（AST，包含 MtrCommand 列表）
    例：42 commands, 12 types (SQL, ECHO, ERROR, LET, ...)
```

解析只执行一次，产出共享给所有 DBMS。

## 2. Execute 阶段

每个 DBMS **并行独立执行**，通过 `ThreadPoolExecutor` 调度：

```
  ┌─ tdsql ──┐  ┌─ mysql ──┐  ┌─ tidb ──┐  ┌─ oceanbase ─┐
  │ MtrExecutor            │  (各 DBMS 各自创建)              │
  │  ├ RosettaDBConnector  →  DBConnection → pymysql        │
  │  ├ VariableStore       →  $var 变量解析                   │
  │  ├ ConnectionManager   →  多连接管理                      │
  │  ├ ErrorHandler        →  --error 预期错误匹配            │
  │  └ ResultProcessor     →  replace_*/sorted/lowercase     │
  └─────────────────────────────────────────────────────────┘
```

### 核心组件

| 组件 | 职责 |
|------|------|
| `RosettaDBConnector` | 适配层，将 `MtrExecutor` 的数据库操作桥接到 rosetta 的 `DBConnection` |
| `VariableStore` | `$var` / `${var}` 变量存储与求值（`let`/`inc`/`dec`/`expr`） |
| `ConnectionManager` | 多连接管理（`connect`/`disconnect`/`connection`/`send`/`reap`） |
| `ErrorHandler` | `--error` 指令的预期错误匹配（错误码/SQLSTATE/错误名） |
| `ResultProcessor` | 结果格式化与变换（`replace_column`/`replace_result`/`replace_regex`/`sorted_result`/`lowercase`） |

### 错误处理策略

跨 DBMS 对比场景下使用 `abort_on_error=False`：

- **预期错误**（`--error` 声明的）：匹配后正常输出 `ERROR: (1062)` 等
- **非预期错误**（如 tdsql 专有变量在其他 DBMS 上报错）：记录 warning，**继续执行后续语句**
- **致命错误**（`--die`/`--exit`）：终止当前 DBMS 的执行

### 输出格式

每条 SQL 和 echo 输出都带 `[L{line_no}]` 行号标签，用于后续比对时 block 对齐：

```
[L5] SELECT 1;
1
[L8] CREATE TABLE t1(id INT);
affected rows: 0
[L3] # Case 1: Test add index
[L42] ALTER TABLE t1 ADD INDEX idx(f1);
affected rows: 0
```

## 3. Compare 阶段

每个 DBMS 执行完毕后产出 `List[str]`（output_lines），写入 `.result` 文件，然后进行两两比对。

### 3.1 比对策略

- **有 baseline**：baseline vs 每个 target 逐一比对
- **无 baseline**：所有 DBMS 两两比对

### 3.2 Block 分割

`comparator.split_into_blocks()` 将 output_lines 按 SQL 语句和 `#` 注释行切割成逻辑块：

```
Block 1: [L5] SELECT 1; / 1
Block 2: [L8] CREATE TABLE t1(id INT); / affected rows: 0
Block 3: [L3] # Case 1: Test add index
Block 4: [L42] ALTER TABLE t1 ADD INDEX idx(f1); / affected rows: 0
```

分割规则：遇到以下行开头时开启新 block：

- `[Lnnn] SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|SHOW|SET|...`
- `#` 注释行

### 3.3 Block 对齐

`comparator._align_blocks()` 按 `[Lnnn]` 行号标签对齐两个 DBMS 的 block：

```
tdsql                    mysql                    对齐结果
─────────────────       ─────────────────        ──────────
[L5] SELECT 1;          [L5] SELECT 1;           ✓ 配对
[L5] SET tdsql_...;     [L5] SET tdsql_...;      ✓ 配对（均输出，但结果不同）
[L12] CREATE TABLE...   [L12] CREATE TABLE...    ✓ 配对
```

**关键**：行号标签保证即使某个 DBMS 跳过了某条语句，后续 block 仍能正确配对，不会错位。

如果两侧都没有 `[Lnnn]` 标签，退回位置对齐（positional alignment）。

### 3.4 Block 比对

对每对齐的 block 逐个比对：

| 情况 | 结果 | 说明 |
|------|------|------|
| 两侧 normalize 后相同 | `matched` | |
| 两侧 normalize 后不同 | `mismatched` | 生成 unified diff |
| 一侧 block 缺失 | `skipped` | 某个 DBMS 跳过了该语句 |
| baseline 有 unexpected error | `skipped` | baseline 本身就出错，不纳入比对 |
| SQL 类型命中自动白名单 | `sql_whitelisted` | 不算错误，黄色展示 |

### 3.6 自动 SQL 白名单

以下 SQL 类型的输出天然跨 DBMS 不一致，自动标记为白名单（`sql_whitelisted`），不算错误但仍然展示（黄色）：

| SQL 类型 | 原因 | 示例 |
|---------|------|------|
| `SHOW CREATE TABLE/VIEW/...` | 不同 DBMS 引擎名、字符集、索引顺序不同 | `SHOW CREATE TABLE t1;` |
| `EXPLAIN` | 执行计划格式因优化器而异 | `EXPLAIN SELECT * FROM t1;` |
| `ANALYZE` | 分析输出格式不同 | `ANALYZE TABLE t1;` |
| `DESCRIBE` / `DESC` | 列描述格式差异 | `DESCRIBE t1;` |
| `SHOW INDEX/VARIABLES/STATUS/...` | 元数据展示差异 | `SHOW INDEX FROM t1;` |
| `SET` | DBMS 专有变量在其他 DBMS 上报 ERROR | `SET tdsql_use_online_copy_ddl = 1;` |

### 3.7 行归一化

`comparator.normalize_line()` 在比对前去除已知噪声：

| 归一化规则 | 示例 |
|-----------|------|
| ERROR 行只保留错误码 | `ERROR 1062 (23000): Duplicate...` → `ERROR: (1062)` |
| 去掉 tdsql tail | `. txid: xxx. sql-node: xxx.` 被移除 |
| ENGINE= 归一化 | `ENGINE=InnoDB` → `ENGINE=<NORMALIZED>` |
| CHARSET= 归一化 | `DEFAULT CHARSET=utf8mb4` → `DEFAULT CHARSET=<NORMALIZED>` |
| 去掉 AUTO_INCREMENT | `AUTO_INCREMENT=123` 被移除 |
| 去掉 ROW_FORMAT | `ROW_FORMAT=Dynamic` 被移除 |
| DEFINER 归一化 | `DEFINER=root@localhost` → `DEFINER=<NORMALIZED>` |
| 过滤 Warning 行 | `Warning 1366 ...` 被移除 |

### 3.8 白名单与 Bug 列表

每个 mismatch 会计算 diff fingerprint，检查：

- **Whitelist**：已知差异，不算失败率
- **Buglist**：已标记为 bug 的差异，仍算失败率

## 4. Report 阶段

| 输出 | 说明 |
|------|------|
| `{test}.{dbms}.result` | 每个 DBMS 的执行结果文件 |
| `{test}.report.txt` | 文本摘要（match/mismatch/skip 统计） |
| `{test}.diff` | 差异详情（unified diff 格式） |
| `{test}.html` | 可视化 HTML 报告（白名单/buglist 标注） |

## 调用链路

### CLI 模式

```
rosetta test -t case.test --dbms tdsql,mysql,tidb
  → cli/run.py::handle_test()
    → RosettaRunner(test_file, configs)
      → .run()
        → ._run_mtr_native()
          → MtrParser.parse()          # 解析
          → ThreadPoolExecutor          # 并行执行
            → MtrExecutor + RosettaDBConnector  # 每个 DBMS
          → .result 文件写入
          → compare_outputs()           # 两两比对
          → _generate_reports()         # 生成报告
```

### 交互模式

```
rosetta -i → MTR Mode → 输入 .test 文件路径
  → InteractiveSession._run_test()
    → RosettaRunner(test_file, configs)
      → .run()  ← 同上
```

两条路径最终都汇聚到 `RosettaRunner.run()` → `_run_mtr_native()`，使用同一套解析和比对逻辑。
