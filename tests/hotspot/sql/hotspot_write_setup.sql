-- Write hotspot dataset setup (isolated from read-hotspot table)
CREATE TABLE hotspot_orders_write (id INT PRIMARY KEY, user_id INT, amount INT, status STRING);

INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (1, 1, 100, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (2, 1, 110, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (3, 1, 120, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (4, 1, 130, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (5, 1, 140, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (6, 1, 150, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (7, 1, 160, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (8, 1, 170, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (9, 2, 80, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (10, 2, 90, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (11, 3, 70, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (12, 3, 60, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (13, 4, 50, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (14, 4, 55, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (15, 5, 65, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (16, 5, 75, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (17, 6, 85, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (18, 6, 95, 'paid');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (19, 7, 105, 'pending');
INSERT INTO hotspot_orders_write (id, user_id, amount, status) VALUES (20, 7, 115, 'paid');

SELECT COUNT(*) AS total_rows FROM hotspot_orders_write;
