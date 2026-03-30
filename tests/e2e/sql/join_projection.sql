DROP TABLE orders;
DROP TABLE users;

CREATE TABLE users (
  id INT PRIMARY KEY,
  name STRING,
  age INT
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  amount INT,
  status STRING
);

INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);

INSERT INTO orders (id, user_id, amount, status) VALUES (101, 1, 80, 'NEW');
INSERT INTO orders (id, user_id, amount, status) VALUES (102, 1, 150, 'PAID');
INSERT INTO orders (id, user_id, amount, status) VALUES (103, 2, 220, 'PAID');

SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100
ORDER BY u.name, o.amount;

SELECT u.name, SUM(o.amount) AS total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount >= 80
GROUP BY u.name
HAVING SUM(o.amount) >= 220
ORDER BY u.name;
