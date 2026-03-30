DROP TABLE products;
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price INT);
INSERT INTO products (id, name, price) VALUES (1, 'A', 10);
INSERT INTO products (id, name, price) VALUES (2, 'B', 20);
SELECT * FROM products ORDER BY id;
