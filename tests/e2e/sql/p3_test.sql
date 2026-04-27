-- ============================================
-- P3 后集成测试
-- ============================================

-- DDL: 建表
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT, email STRING);

CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  name VARCHAR(100),
  price DOUBLE,
  stock INT,
  description TEXT
);

CREATE TABLE orders (
  user_id INT,
  order_id INT,
  product STRING,
  amount DOUBLE,
  status STRING,
  PRIMARY KEY ((user_id), order_id)
);

SHOW TABLES;

-- DML: INSERT
INSERT INTO users (id, name, age, email) VALUES (1, 'alice', 25, 'alice@example.com');
INSERT INTO users (id, name, age, email) VALUES (2, 'bob', 30, 'bob@example.com');
INSERT INTO users (id, name, age, email) VALUES (3, 'carol', 28, 'carol@example.com');

INSERT INTO products (id, name, price, stock, description) VALUES (1, 'Laptop', 999.99, 50, 'High-performance laptop');
INSERT INTO products (id, name, price, stock, description) VALUES (2, 'Mouse', 29.99, 200, 'Wireless mouse');
INSERT INTO products (id, name, price, stock, description) VALUES (3, 'Keyboard', 79.99, 150, 'Mechanical keyboard');

INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (1, 101, 'Laptop', 999.99, 'paid');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (1, 102, 'Mouse', 29.99, 'paid');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (2, 201, 'Keyboard', 79.99, 'pending');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (3, 301, 'Mouse', 29.99, 'paid');

-- UPDATE / DELETE
UPDATE users SET age = 26 WHERE id = 1;
DELETE FROM orders WHERE user_id = 3 AND order_id = 301;

-- SELECT 基础
SELECT * FROM users;
SELECT id, name, email FROM users WHERE age > 25;
SELECT * FROM users WHERE name LIKE 'a%';

-- ORDER BY + LIMIT
SELECT * FROM products ORDER BY price DESC;
SELECT * FROM products ORDER BY price DESC LIMIT 2;

-- 聚合
SELECT COUNT(*) FROM orders;
SELECT SUM(amount) FROM orders;
SELECT AVG(amount), MAX(amount), MIN(amount) FROM orders;

-- GROUP BY + HAVING
SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id;
SELECT user_id, SUM(amount) AS total FROM orders WHERE status = 'paid' GROUP BY user_id HAVING SUM(amount) > 50;

-- JOIN
SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id;
SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC;

-- 清理
DROP TABLE orders;
DROP TABLE products;
DROP TABLE users;
SHOW TABLES;
