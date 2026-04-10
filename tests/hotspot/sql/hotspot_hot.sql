-- Hot path: repeatedly hit the same logical key range (user_id = 1)
SELECT COUNT(*) AS c FROM hotspot_orders WHERE user_id = 1;
SELECT SUM(amount) AS s FROM hotspot_orders WHERE user_id = 1;
SELECT COUNT(*) AS paid_c FROM hotspot_orders WHERE user_id = 1 AND status = 'paid';
