-- Warm path: medium skew across a few users
SELECT COUNT(*) AS c2 FROM hotspot_orders WHERE user_id = 2;
SELECT COUNT(*) AS c3 FROM hotspot_orders WHERE user_id = 3;
SELECT COUNT(*) AS c4 FROM hotspot_orders WHERE user_id = 4;
SELECT COUNT(*) AS c5 FROM hotspot_orders WHERE user_id = 5;
SELECT COUNT(*) AS c6 FROM hotspot_orders WHERE user_id = 6;
