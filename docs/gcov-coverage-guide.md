# SQLEngine 代码测试覆盖率指南（gcov）

## 概述

项目已内建完整的 gcov 覆盖率支持，通过 `make.sh -G 1` 编译后运行 MTR 测试即可生成覆盖率报告。

## 工具依赖

| 工具 | 最低版本 | 安装方式 | 说明 |
|------|---------|---------|------|
| GCC | >= 9 | 系统自带 | 编译器 |
| gcov | >= 9 | 随 GCC 自带 | 覆盖率数据收集 |
| fastcov | - | `pip install fastcov` | 快速解析 gcov 数据 |
| genhtml | - | `apt install lcov` / `yum install lcov` | 生成 HTML 报告 |

> 如果使用 Clang 编译器，cmake 会自动使用 `llvm-cov gcov` 替代 gcov。

## 操作流程

### Step 1: 编译（启用 gcov 插桩）

```bash
./make.sh -G 1 -d 1 -m 1
```

| 参数 | 含义 |
|------|------|
| `-G 1` / `--enable-gcov` | 启用 gcov 覆盖率插桩 |
| `-d 1` | Debug 模式（gcov 推荐） |
| `-m 1` | 构建 MTR 测试二进制 |

编译后，每个 `.cc` 文件会在 build 目录生成对应的 `.gcno` 文件（编译时控制流图）。

### Step 2: 清零覆盖率计数器

```bash
cd <build_dir>
make fastcov-clean
```

清除之前运行留下的 `.gcda` 计数文件，确保干净的基线。

### Step 3: 运行 MTR 测试

```bash
cd mysql-test

# 运行单个测试
./mtr --suite=tdsql/json json_multivalue_index_rocksdb_load_data

# 运行整个 suite
./mtr --suite=tdsql/json

# 运行多个指定测试
./mtr --suite=tdsql/json json_multivalue_index_rocksdb_load_data json_multivalue_index_rocksdb_ddl json_multivalue_index_rocksdb_uk
```

运行过程中，每个被执行的代码路径会在 `.gcda` 文件中累加计数。

### Step 4: 生成覆盖率报告

```bash
cd <build_dir>

# 生成 lcov 格式报告
make fastcov-report    # → 输出 report.info

# 生成 HTML 可视化报告
make fastcov-html      # → 输出 code_coverage/ 目录
```

### Step 5: 查看报告

```bash
# 浏览器打开 HTML 报告
# code_coverage/index.html 是入口页面

# 命令行查看总体覆盖率
lcov --summary report.info

# 提取单个文件的覆盖率
lcov --extract report.info '*/ha_rocksdb.cc' -o ha_rocksdb.info
genhtml ha_rocksdb.info -o ha_rocksdb_coverage/

# 提取某个目录的覆盖率
lcov --extract report.info '*/storage/rocksdb/*' -o rocksdb.info
genhtml rocksdb.info -o rocksdb_coverage/
```

## 注意事项

1. **性能影响**：gcov 构建的二进制约慢 2-5x，不要用于性能测试
2. **编译时间**：每个编译单元多了 `-fprofile-arcs -ftest-coverage` 插桩，编译时间会增加
3. **磁盘空间**：`.gcno` + `.gcda` 文件可能占用数 GB，build 目录要留足空间
4. **精确覆盖率**：只运行特定测试可以得到该测试的精确覆盖率；运行全量 MTR 则得到整体覆盖率
5. **增量覆盖率**：不执行 `make fastcov-clean` 的情况下多次运行测试，`.gcda` 会累加，最终报告反映所有运行的总和

## MVI 相关测试覆盖率分析结论

以下是对 Multi-Valued Index (MVI) 相关代码的覆盖率分析结果：

### 已覆盖的路径

| 代码位置 | 功能 | 覆盖测试 |
|---------|------|---------|
| `rdb_datadic.cc:988` `pack_record` 的 `mv_null_placeholder` 逻辑 | MVI NULL/空数组占位符 packing | `json_multivalue_index_rocksdb_null` |
| `ha_rocksdb.cc` `write_mv_null_placeholder` / `delete_mv_null_placeholder` | MVI NULL 占位符写入/删除 | MVI NULL 测试 |
| `ha_rocksdb.cc` `write_multi_valued_sk` | 正常 MVI 二级键写入 | 所有 MVI DML 测试 |
| `ha_rocksdb.cc` `update_multi_valued_sk` | MVI 二级键更新 | MVI UPDATE 测试 |
| `ha_rocksdb.cc` `extract_multi_valued_elements` | JSON 数组元素提取 | 所有 MVI 操作 |
| `load_data.cc` `mv_uk_mp_` 相关逻辑 | LOAD DATA 批量优化的 MVI UK 处理 | `json_multivalue_index_rocksdb_load_data` Section 13-19 |
| `rpc_batch_cntl.cc` `Reaggregate` MVI 去重 | 批量检查重新聚合的 MVI 去重 | LOAD DATA + UNIQUE MVI 测试 |

### 未覆盖但符合预期的路径

| 代码位置 | 原因 |
|---------|------|
| `rdb_datadic.h:424-425` `pack_record` LazyBuffer 重载的 `mv_null_placeholder` 参数 | 转发函数，被 inline 或走另一个重载 |
| `ha_rocksdb.cc:8708-8709` 原 `unpack_record` 中的 MVI 分支 | **已删除**——不可达的防御代码 |

### 原未覆盖、已补充测试的路径

| 代码位置 | 功能 | 补充的测试 |
|---------|------|-----------|
| `load_data.cc` `mv_uk_mp_` 全部路径 (AddKV / CheckDuplicate / CheckDuplicateWithDeleteRows / DeleteOldRows / FillAndFlush) | LOAD DATA + 复合唯一 MVI | `json_multivalue_index_rocksdb_load_data` Section 13-18 |
| `load_data.cc` Section 19 路径 | LOAD DATA REPLACE + 旧行 NULL/空数组 MVI | `json_multivalue_index_rocksdb_load_data` Section 19 |
| `ha_rocksdb.cc:8800-8808` `batch_delete_mvi_sk_keys` 的 `elements.empty()` 分支 | 批量删除旧行 MVI SK 时的 NULL placeholder 删除 | `json_multivalue_index_rocksdb_load_data` Section 19 |
| `ha_rocksdb.cc:9758-9843` `write_multi_valued_sk_with_callback` | DDL fillback 写入 MVI 索引（含 NULL/空数组占位符） | `json_multivalue_index_rocksdb_ddl` Section 8 |
