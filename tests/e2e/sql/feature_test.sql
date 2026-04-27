-- Cleanup from previous runs
DROP TABLE orders;
DROP TABLE users;
-- DDL
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);
CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);
-- INSERT
INSERT INTO users (id, name, age) VALUES (1, 'alice', 25);
INSERT INTO users (id, name, age) VALUES (2, 'bob', 30);
INSERT INTO users (id, name, age) VALUES (3, 'carol', 28);
INSERT INTO users (id, name, age) VALUES (4, 'dave', 35);
INSERT INTO users (id, name, age) VALUES (5, 'eve', 22);
INSERT INTO orders (id, user_id, amount, status) VALUES (1, 1, 100.0, 'paid');
INSERT INTO orders (id, user_id, amount, status) VALUES (2, 2, 200.0, 'pending');
INSERT INTO orders (id, user_id, amount, status) VALUES (3, 1, 50.0, 'paid');
INSERT INTO orders (id, user_id, amount, status) VALUES (4, 3, 150.0, 'paid');
-- Baseline SELECTs
SELECT * FROM users;
SELECT * FROM orders;
-- UPDATE with full primary key
UPDATE users SET age = 26 WHERE id = 1;
SELECT id, name, age FROM users WHERE id = 1;
-- DELETE with full primary key
DELETE FROM orders WHERE id = 4;
SELECT COUNT(*) FROM orders;
-- UPDATE with partial key (non-key column WHERE)
UPDATE users SET age = 99 WHERE name = 'bob';
SELECT id, name, age FROM users WHERE name = 'bob';
-- DELETE with partial key (no primary key in WHERE)
DELETE FROM orders WHERE status = 'pending';
SELECT id, user_id, amount, status FROM orders;
-- LIKE
SELECT * FROM users WHERE name LIKE 'a%';
-- Aggregate functions
SELECT COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM orders;
-- GROUP BY
SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total FROM orders GROUP BY user_id;
-- HAVING
SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING SUM(amount) > 60;
-- INNER JOIN
SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;
-- LEFT JOIN
SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id;
-- Cleanup
DROP TABLE orders;
DROP TABLE users;
