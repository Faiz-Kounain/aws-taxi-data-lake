-- ---------------- Raw data checks ----------------

-- rows per month
SELECT month, COUNT(*) AS trips
FROM taxi_db.raw_yellow_trips
GROUP BY month ORDER BY month;

-- Partition pruning demo: compare "Data scanned" under each result
SELECT COUNT(*) FROM taxi_db.raw_yellow_trips;                   -- all 6 months
SELECT COUNT(*) FROM taxi_db.raw_yellow_trips WHERE month = '03'; -- only the March folder

-- ---------------- Business questions (gold) ----------------

-- Q1. Which 5 zones make the most money?
SELECT pickup_zone, SUM(trips) AS trips, ROUND(SUM(revenue)) AS revenue
FROM taxi_db.gold_daily_zone_revenue
GROUP BY pickup_zone
ORDER BY revenue DESC
LIMIT 5;

-- Q2. What are the busiest hours?
SELECT pickup_hour, SUM(trips) AS trips
FROM taxi_db.gold_hourly_demand
GROUP BY pickup_hour ORDER BY trips DESC LIMIT 5;

-- Q3. Card or cash, and who tips more?
SELECT payment, SUM(trips) AS trips, ROUND(AVG(avg_tip_pct), 1) AS tip_pct
FROM taxi_db.gold_payment_tips
GROUP BY payment ORDER BY trips DESC;

-- ---------------- Pipeline health ----------------
SELECT * FROM taxi_db.framework_audit ORDER BY started DESC;
