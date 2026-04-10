-- Uniform path: spread reads across many keys
SELECT COUNT(*) AS c01 FROM hotspot_orders WHERE user_id = 1;
SELECT COUNT(*) AS c02 FROM hotspot_orders WHERE user_id = 2;
SELECT COUNT(*) AS c03 FROM hotspot_orders WHERE user_id = 3;
SELECT COUNT(*) AS c04 FROM hotspot_orders WHERE user_id = 4;
SELECT COUNT(*) AS c05 FROM hotspot_orders WHERE user_id = 5;
SELECT COUNT(*) AS c06 FROM hotspot_orders WHERE user_id = 6;
SELECT COUNT(*) AS c07 FROM hotspot_orders WHERE user_id = 7;
SELECT COUNT(*) AS c08 FROM hotspot_orders WHERE user_id = 8;
SELECT COUNT(*) AS c09 FROM hotspot_orders WHERE user_id = 9;
SELECT COUNT(*) AS c10 FROM hotspot_orders WHERE user_id = 10;
SELECT COUNT(*) AS c11 FROM hotspot_orders WHERE user_id = 11;
SELECT COUNT(*) AS c12 FROM hotspot_orders WHERE user_id = 12;
