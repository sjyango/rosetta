# 无主键表 Online DDL 测试用例

> 基础表结构：
> ```sql
> CREATE TABLE bench_no_pk (
>     id INT NOT NULL, user_id INT NOT NULL, data JSON NOT NULL,
>     category VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL,
>     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
>     KEY idx_id (id), KEY idx_user (user_id)
> );
> ```

---

## 一、ADD COLUMN

| # | DDL | 说明 |
|---|-----|------|
| 1 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_score INT DEFAULT 0;` | 新增 INT 列带默认值 |
| 2 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_big_id BIGINT DEFAULT 0;` | 新增 BIGINT 列 |
| 3 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_remark VARCHAR(100) DEFAULT '';` | 新增 VARCHAR 列 |
| 4 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_updated DATETIME DEFAULT '2024-01-01 00:00:00';` | 新增 DATETIME 列 |
| 5 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_amount DECIMAL(10,2) DEFAULT 99.99;` | 新增 DECIMAL 列 |
| 6 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_content TEXT;` | 新增 TEXT 列 |
| 7 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_flag TINYINT NOT NULL DEFAULT 0;` | 新增 NOT NULL 列 |
| 8 | `ALTER TABLE bench_no_pk ADD COLUMN tmp_a INT DEFAULT 1, ADD COLUMN tmp_b VARCHAR(50) DEFAULT 'x';` | 一次新增多列 |

## 二、DROP COLUMN

| # | DDL | 说明 |
|---|-----|------|
| 9 | `ALTER TABLE bench_no_pk DROP COLUMN category;` | 删除普通列 |
| 10 | `ALTER TABLE bench_no_pk DROP COLUMN status;` | 删除带默认值的列 |

## 三、MODIFY COLUMN

| # | DDL | 说明 |
|---|-----|------|
| 11 | `ALTER TABLE bench_no_pk MODIFY COLUMN status VARCHAR(100) NOT NULL;` | 扩大 VARCHAR 宽度 |
| 12 | `ALTER TABLE bench_no_pk MODIFY COLUMN status VARCHAR(10) NOT NULL;` | 缩小 VARCHAR 宽度（可能截断） |
| 13 | `ALTER TABLE bench_no_pk MODIFY COLUMN user_id BIGINT NOT NULL;` | INT → BIGINT 类型提升 |
| 14 | `ALTER TABLE bench_no_pk MODIFY COLUMN user_id TINYINT NOT NULL;` | INT → TINYINT 类型缩小（可能溢出） |
| 15 | `ALTER TABLE bench_no_pk MODIFY COLUMN category VARCHAR(50) NULL;` | 添加/移除 NOT NULL |
| 16 | `ALTER TABLE bench_no_pk MODIFY COLUMN status VARCHAR(20) NOT NULL DEFAULT 'active';` | 修改默认值 |
| 17 | `ALTER TABLE bench_no_pk MODIFY COLUMN status VARCHAR(20) NOT NULL;` | 移除默认值 |

## 四、CHANGE COLUMN

| # | DDL | 说明 |
|---|-----|------|
| 18 | `ALTER TABLE bench_no_pk CHANGE COLUMN category cat VARCHAR(50) NOT NULL;` | 重命名列 |

## 五、ADD INDEX

| # | DDL | 说明 |
|---|-----|------|
| 19 | `ALTER TABLE bench_no_pk ADD KEY idx_category (category);` | 普通二级索引 |
| 20 | `ALTER TABLE bench_no_pk ADD KEY idx_cat_status (category, status);` | 复合索引 |
| 21 | `ALTER TABLE bench_no_pk ADD KEY idx_cat_prefix (category(10));` | 前缀索引 |
| 22 | `ALTER TABLE bench_no_pk ADD KEY idx_json_length ((JSON_LENGTH(data)));` | 函数索引 |
| 23 | `ALTER TABLE bench_no_pk ADD KEY idx_mv_data ((CAST(data->'$' AS CHAR(64) ARRAY)));` | 多值索引 |
| 24 | `ALTER TABLE bench_no_pk ADD KEY idx_mv_int ((CAST(data->'$' AS SIGNED ARRAY)));` | 整数多值索引 |
| 25 | `ALTER TABLE bench_no_pk ADD KEY idx_mv_composite ((CAST(data->'$' AS CHAR(64) ARRAY)), category);` | 复合多值索引 |
| 26 | `CREATE INDEX idx_status ON bench_no_pk (status);` | CREATE INDEX 语法 |

## 六、DROP INDEX

| # | DDL | 说明 |
|---|-----|------|
| 27 | `ALTER TABLE bench_no_pk DROP KEY idx_user;` | 删除普通二级索引 |
| 28 | `DROP INDEX idx_id ON bench_no_pk;` | DROP INDEX 语法 |
| 29 | `ALTER TABLE bench_no_pk DROP KEY idx_mv_data;` | 删除多值索引（需先添加） |

## 七、虚拟列 / 生成列

| # | DDL | 说明 |
|---|-----|------|
| 30 | `ALTER TABLE bench_no_pk ADD COLUMN arr_len INT AS (JSON_LENGTH(data)) VIRTUAL;` | 新增 VIRTUAL 虚拟列 |
| 31 | `ALTER TABLE bench_no_pk ADD COLUMN cat_upper VARCHAR(50) AS (UPPER(category)) STORED;` | 新增 STORED 生成列 |
| 32 | `ALTER TABLE bench_no_pk ADD KEY idx_arr_len (arr_len);` | 在虚拟列上建索引 |
| 33 | `ALTER TABLE bench_no_pk DROP COLUMN arr_len;` | 删除虚拟列 |

## 八、无主键特有测试（TDSQL 限制验证）

| # | DDL | 说明 |
|---|-----|------|
| 34 | `ALTER TABLE bench_no_pk ADD PRIMARY KEY (id);` | 添加 PRIMARY KEY（TDSQL online copy 应报错 8528） |
| 35 | `ALTER TABLE bench_no_pk ADD UNIQUE KEY uk_id (id);` | 添加 UNIQUE NOT NULL 索引（TDSQL 应报错 8528） |
| 36 | `ALTER TABLE bench_no_pk ADD UNIQUE KEY uk_category (category);` | 添加 UNIQUE 允许 NULL 的索引（应成功） |
| 37 | `SET tdsql_use_online_copy_ddl = OFF; ALTER TABLE bench_no_pk ADD PRIMARY KEY (id); SET tdsql_use_online_copy_ddl = ON; ALTER TABLE bench_no_pk ADD COLUMN tmp_with_pk INT DEFAULT 0;` | 先加 PK 再做 DDL（对比有 PK 后行为） |

## 九、其他 DDL

| # | DDL | 说明 |
|---|-----|------|
| 38 | `ALTER TABLE bench_no_pk FORCE;` | ALTER TABLE FORCE（重建表） |
| 39 | `ALTER TABLE bench_no_pk;` | 空 ALTER（仅验证表结构） |
| 40 | `ALTER TABLE bench_no_pk RENAME TO bench_no_pk_renamed;` | RENAME TABLE |
