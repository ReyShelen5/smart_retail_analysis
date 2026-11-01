-- Step 12: Hive over Kafka-landed real-time sales
USE retail_db;

DROP TABLE IF EXISTS sales_stream;
CREATE EXTERNAL TABLE sales_stream (
    transaction_id    STRING,
    transaction_date  DATE,
    transaction_time  STRING,
    store_id          STRING,
    store_city        STRING,
    product_id        STRING,
    product_name      STRING,
    category          STRING,
    quantity          INT,
    unit_price        DECIMAL(10,2),
    cost_price        DECIMAL(10,2),
    discount_pct      INT,
    total_amount      DECIMAL(12,2),
    profit            DECIMAL(12,2),
    payment_mode      STRING
)
COMMENT 'Real-time sales landed from Kafka as micro-batches'
PARTITIONED BY (sale_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'file://${hivevar:PROJECT_DIR}/data/streaming';

-- Register partition folders written by the consumer
MSCK REPAIR TABLE sales_stream;
SHOW PARTITIONS sales_stream;

-- 12.1 Real-time KPIs
SELECT COUNT(*) AS live_transactions, SUM(quantity) AS units,
       ROUND(SUM(total_amount), 2) AS revenue, ROUND(SUM(profit), 2) AS profit,
       MIN(transaction_time) AS first_seen, MAX(transaction_time) AS last_seen
FROM sales_stream;

-- 12.2 Live revenue by store and category
SELECT store_city, category, COUNT(*) AS transactions,
       ROUND(SUM(total_amount), 2) AS revenue
FROM sales_stream
GROUP BY store_city, category
ORDER BY revenue DESC
LIMIT 10;

-- 12.3 Per-minute sales trend
SELECT substr(transaction_time, 1, 5) AS minute, COUNT(*) AS transactions,
       ROUND(SUM(total_amount), 2) AS revenue
FROM sales_stream
GROUP BY substr(transaction_time, 1, 5)
ORDER BY minute DESC
LIMIT 10;

-- 12.4 Unified view: historical + real-time
CREATE OR REPLACE VIEW sales_all AS
SELECT transaction_id, transaction_date, store_id, store_city, product_id, product_name,
       category, quantity, total_amount, profit, payment_mode, 'historical' AS source
FROM sales
UNION ALL
SELECT transaction_id, transaction_date, store_id, store_city, product_id, product_name,
       category, quantity, total_amount, profit, payment_mode, 'realtime' AS source
FROM sales_stream;

SELECT source, COUNT(*) AS transactions, ROUND(SUM(total_amount), 2) AS revenue
FROM sales_all
GROUP BY source;
