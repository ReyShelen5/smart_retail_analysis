-- Query optimization: statistics, vectorization, CBO + benchmark
USE retail_db;

SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.cbo.enable = true;
SET hive.optimize.ppd = true;
SET hive.compute.query.using.stats = true;
SET hive.stats.fetch.column.stats = true;
SET hive.fetch.task.conversion = more;

-- Collect statistics for the cost-based optimizer
ANALYZE TABLE sales COMPUTE STATISTICS;
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE sales_partitioned PARTITION (sale_date) COMPUTE STATISTICS;

-- Same question asked of 3 storage layouts:
-- "Revenue per store on 2026-06-15"

-- B1: raw CSV text table (full scan + casting every row)
SELECT store_city, ROUND(SUM(CAST(total_amount AS DECIMAL(12,2))), 2) AS revenue
FROM sales_raw WHERE transaction_date = '2026-06-15'
GROUP BY store_city;

-- B2: ORC table (columnar, but still scans all dates)
SELECT store_city, ROUND(SUM(total_amount), 2) AS revenue
FROM sales WHERE transaction_date = DATE '2026-06-15'
GROUP BY store_city;

-- B3: partitioned ORC table (reads one partition)
SELECT store_city, ROUND(SUM(total_amount), 2) AS revenue
FROM sales_partitioned WHERE sale_date = '2026-06-15'
GROUP BY store_city;

-- B4: COUNT(*) answered straight from metastore statistics (no job)
SELECT COUNT(*) FROM sales;
