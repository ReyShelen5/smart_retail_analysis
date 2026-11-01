-- Step 5: Create the Hive sales table
USE retail_db;

-- 5.1 External staging table over the raw CSV
DROP TABLE IF EXISTS sales_raw;
CREATE EXTERNAL TABLE sales_raw (
    transaction_id    STRING,
    transaction_date  STRING,
    transaction_time  STRING,
    store_id          STRING,
    store_city        STRING,
    product_id        STRING,
    product_name      STRING,
    category          STRING,
    quantity          STRING,
    unit_price        STRING,
    cost_price        STRING,
    discount_pct      STRING,
    total_amount      STRING,
    profit            STRING,
    payment_mode      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'file://${hivevar:PROJECT_DIR}/data/historical'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 5.2 Typed, ORC-backed analytics table
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
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
COMMENT 'Historical retail sales transactions'
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE sales
SELECT
    transaction_id,
    CAST(transaction_date AS DATE),
    transaction_time,
    store_id,
    store_city,
    product_id,
    product_name,
    category,
    CAST(quantity      AS INT),
    CAST(unit_price    AS DECIMAL(10,2)),
    CAST(cost_price    AS DECIMAL(10,2)),
    CAST(discount_pct  AS INT),
    CAST(total_amount  AS DECIMAL(12,2)),
    CAST(profit        AS DECIMAL(12,2)),
    payment_mode
FROM sales_raw;

-- 5.3 Verify
SHOW TABLES;
SELECT COUNT(*) AS total_rows FROM sales;
SELECT * FROM sales LIMIT 5;
SELECT MIN(transaction_date) AS first_day, MAX(transaction_date) AS last_day FROM sales;
