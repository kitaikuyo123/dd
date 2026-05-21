-- ============================================================
-- MiniSQL 完整测试 SQL 脚本
-- 用法: minisql> source docs/test_all.sql
-- ============================================================

-- ============================================================
-- 第 8 章 DDL 测试 (DDL-01 ~ DDL-09)
-- ============================================================

-- DDL-01 创建单主键表
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);

-- DDL-02 创建 DOUBLE 字段表
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price DOUBLE);

-- DDL-03 创建 VARCHAR 表
CREATE TABLE users2 (id INT PRIMARY KEY, name VARCHAR(100));

-- DDL-04 创建复合主键表 (不支持复合主键，期望报错)
-- CREATE TABLE user_orders (
--   user_id INT,
--   order_id INT,
--   product STRING,
--   amount DOUBLE,
--   status STRING,
--   PRIMARY KEY ((user_id), order_id)
-- );

-- DDL-05 查看表列表
SHOW TABLES;

-- DDL-06 重复创建同名表 (期望报错)
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);

-- DDL-07 删除表
DROP TABLE users2;

-- DDL-08 删除不存在表 (期望报错或兼容行为)
DROP TABLE missing_table;

-- DDL-09 删除后重建
DROP TABLE products;
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price DOUBLE);

-- ============================================================
-- 创建辅助表（用于后续 JOIN 和聚合测试）
-- ============================================================

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);

-- ============================================================
-- 第 9 章 DML 测试 (DML-01 ~ DML-12)
-- ============================================================

-- DML-01 单行 INSERT
INSERT INTO users (id, name, age) VALUES (1, 'alice', 25);

-- DML-02 多行连续 INSERT
INSERT INTO users (id, name, age) VALUES (2, 'bob', 30);
INSERT INTO users (id, name, age) VALUES (3, 'charlie', 35);
INSERT INTO users (id, name, age) VALUES (4, 'diana', 28);

-- DML-10 类型转换 — 插入不同类型数据
INSERT INTO products (id, name, price) VALUES (1, 'laptop', 5999.99);
INSERT INTO products (id, name, price) VALUES (2, 'phone', 3299.50);
INSERT INTO products (id, name, price) VALUES (3, 'tablet', 2599.00);

INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 150.50, 'shipped');
INSERT INTO orders (id, user_id, amount, status) VALUES (2, 1, 89.90, 'pending');
INSERT INTO orders (id, user_id, amount, status) VALUES (3, 2, 299.00, 'shipped');
INSERT INTO orders (id, user_id, amount, status) VALUES (4, 2, 45.00, 'pending');
INSERT INTO orders (id, user_id, amount, status) VALUES (5, 3, 520.00, 'delivered');
INSERT INTO orders (id, user_id, amount, status) VALUES (6, 4, 178.50, 'shipped');

-- DML-03 主键查询验证插入
SELECT * FROM users WHERE id = 1;

-- DML-04 完整主键 UPDATE
UPDATE users SET age = 26 WHERE id = 1;

-- 验证更新结果
SELECT * FROM users WHERE id = 1;

-- DML-05 非主键 UPDATE
UPDATE users SET age = 99 WHERE name = 'bob';

-- 验证更新结果
SELECT * FROM users WHERE name = 'bob';

-- DML-06 完整主键 DELETE
DELETE FROM orders WHERE id = 3;

-- 验证删除结果 (id=3 不应出现)
SELECT * FROM orders;

-- DML-07 非主键 DELETE
DELETE FROM orders WHERE status = 'pending';

-- 验证删除结果 (pending 状态的行不应出现)
SELECT * FROM orders;

-- DML-08 删除不存在行 (期望合理 affected rows)
DELETE FROM users WHERE id = 999;

-- DML-09 更新不存在行 (期望合理 affected rows)
UPDATE users SET age = 1 WHERE id = 999;

-- DML-11 非法类型插入 (期望报错)
INSERT INTO users (id, name, age) VALUES (5, 'wrong', 'not_a_number');

-- DML-12 主键重复插入 (期望覆盖或报错，记录行为)
INSERT INTO users (id, name, age) VALUES (1, 'alice_v2', 27);

-- 验证重复插入后结果
SELECT * FROM users WHERE id = 1;

-- ============================================================
-- 第 10 章 SELECT 查询测试 (QUERY-01 ~ QUERY-29)
-- ============================================================

-- QUERY-01 全表查询
SELECT * FROM users;

-- QUERY-02 列投影
SELECT id, name FROM users;

-- QUERY-03 等值过滤
SELECT * FROM users WHERE id = 1;

-- QUERY-04 比较过滤
SELECT * FROM users WHERE age > 25;

-- QUERY-05 AND 条件
SELECT * FROM users WHERE age > 20 AND name = 'alice_v2';

-- QUERY-06 OR 条件
SELECT * FROM users WHERE name = 'alice_v2' OR name = 'bob';

-- QUERY-07 LIKE
SELECT * FROM users WHERE name LIKE 'a%';

-- QUERY-08 ORDER BY ASC
SELECT * FROM users ORDER BY age ASC;

-- QUERY-09 ORDER BY DESC
SELECT * FROM users ORDER BY age DESC;

-- QUERY-10 LIMIT
SELECT * FROM users LIMIT 2;

-- QUERY-11 LIMIT OFFSET
SELECT * FROM users LIMIT 2 OFFSET 1;

-- QUERY-12 COUNT
SELECT COUNT(*) FROM orders;

-- QUERY-13 SUM/AVG/MAX/MIN
SELECT SUM(amount) FROM orders;
SELECT AVG(amount) FROM orders;
SELECT MAX(amount) FROM orders;
SELECT MIN(amount) FROM orders;

-- QUERY-14 GROUP BY
SELECT user_id, COUNT(*) FROM orders GROUP BY user_id;

-- QUERY-15 HAVING
SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING total > 60;

-- QUERY-16 INNER JOIN
SELECT u.id, u.name, o.amount, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id;

-- QUERY-17 LEFT JOIN
SELECT u.id, u.name, o.amount, o.status FROM users u LEFT JOIN orders o ON u.id = o.user_id;

-- QUERY-18 表别名
SELECT u.id, u.name FROM users u WHERE u.age > 25;

-- QUERY-19 列别名
SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id;

-- QUERY-20 空结果集
SELECT * FROM users WHERE id = 999;

-- QUERY-21 DISTINCT
SELECT DISTINCT status FROM orders;

-- QUERY-22 BETWEEN
SELECT * FROM orders WHERE amount BETWEEN 50 AND 200;

-- QUERY-23 NOT BETWEEN
SELECT * FROM orders WHERE amount NOT BETWEEN 50 AND 200;

-- QUERY-24 IN 字面量列表
SELECT * FROM orders WHERE status IN ('shipped', 'pending');

-- QUERY-25 NOT IN
SELECT * FROM orders WHERE status NOT IN ('shipped');

-- QUERY-26 IS NULL / IS NOT NULL
SELECT * FROM users WHERE name IS NOT NULL;
SELECT * FROM users WHERE name IS NULL;

-- QUERY-27 NOT 运算符
SELECT * FROM orders WHERE NOT status = 'shipped';

-- QUERY-28 RIGHT JOIN
SELECT u.id, u.name, o.amount, o.status FROM orders o RIGHT JOIN users u ON o.user_id = u.id;

-- QUERY-29 FULL JOIN
SELECT u.id, u.name, o.amount, o.status FROM users u FULL JOIN orders o ON u.id = o.user_id;

-- ============================================================
-- 第 24 章 异常输入与错误处理测试 (ERR-01 ~ ERR-07)
-- ============================================================

-- ERR-01 SQL 语法错误 (期望报错，CLI 不退出)
SELEC * FROM users;

-- ERR-02 查询不存在表 (期望报错)
SELECT * FROM missing;

-- ERR-03 查询不存在列 (期望报错)
SELECT bad_col FROM users;

-- ERR-04 类型不匹配 (期望报错或可解释行为)
SELECT * FROM users WHERE id = 'abc';

-- ============================================================
-- 第 25 章 数据一致性测试 (CONS-01 ~ CONS-03)
-- ============================================================

-- CONS-01 写后读一致性
INSERT INTO users (id, name, age) VALUES (10, 'cons_test', 20);
SELECT * FROM users WHERE id = 10;

-- CONS-02 更新后读一致性
UPDATE users SET age = 21 WHERE id = 10;
SELECT * FROM users WHERE id = 10;

-- CONS-03 删除后读一致性
DELETE FROM users WHERE id = 10;
SELECT * FROM users WHERE id = 10;

-- ============================================================
-- 清理测试数据（可选）
-- ============================================================

DROP TABLE orders;
DROP TABLE products;
DROP TABLE users;
