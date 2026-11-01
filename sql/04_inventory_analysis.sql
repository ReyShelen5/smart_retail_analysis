-- Steps 7-9: Inventory table, restocking alerts, sales x inventory analysis
USE retail_db;

-------------------------------------------------------------------
-- Step 7: Create the inventory table
-------------------------------------------------------------------
DROP TABLE IF EXISTS inventory_raw;
CREATE EXTERNAL TABLE inventory_raw (
    product_id         STRING,
    product_name       STRING,
    category           STRING,
    store_id           STRING,
    current_stock      STRING,
    reorder_level      STRING,
    max_stock          STRING,
    unit_cost          STRING,
    last_restock_date  STRING,
    supplier           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'file://${hivevar:PROJECT_DIR}/data/inventory'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory (
    product_id         STRING,
    product_name       STRING,
    category           STRING,
    store_id           STRING,
    current_stock      INT,
    reorder_level      INT,
    max_stock          INT,
    unit_cost          DECIMAL(10,2),
    last_restock_date  DATE,
    supplier           STRING
)
COMMENT 'Current stock levels per store and product'
STORED AS ORC;

INSERT OVERWRITE TABLE inventory
SELECT
    product_id, product_name, category, store_id,
    CAST(current_stock AS INT),
    CAST(reorder_level AS INT),
    CAST(max_stock     AS INT),
    CAST(unit_cost     AS DECIMAL(10,2)),
    CAST(last_restock_date AS DATE),
    supplier
FROM inventory_raw;

SELECT COUNT(*) AS inventory_rows FROM inventory;
SELECT * FROM inventory LIMIT 5;

-------------------------------------------------------------------
-- Step 8: Identify products requiring restocking
-------------------------------------------------------------------
DROP TABLE IF EXISTS restock_alerts;
CREATE TABLE restock_alerts STORED AS ORC AS
SELECT
    store_id,
    product_id,
    product_name,
    category,
    current_stock,
    reorder_level,
    max_stock,
    CASE
        WHEN current_stock = 0                    THEN 'OUT_OF_STOCK'
        WHEN current_stock < reorder_level * 0.5  THEN 'CRITICAL'
        ELSE                                           'LOW'
    END                                            AS stock_status,
    (max_stock - current_stock)                    AS suggested_order_qty,
    ROUND((max_stock - current_stock) * unit_cost, 2) AS estimated_order_cost,
    supplier
FROM inventory
WHERE current_stock < reorder_level;

-- 8.1 All alerts, most urgent first
SELECT store_id, product_id, product_name, category, current_stock,
       reorder_level, stock_status, suggested_order_qty, estimated_order_cost
FROM restock_alerts
ORDER BY
    CASE stock_status WHEN 'OUT_OF_STOCK' THEN 1 WHEN 'CRITICAL' THEN 2 ELSE 3 END,
    estimated_order_cost DESC
LIMIT 20;

-- 8.2 Alert summary per store
SELECT
    store_id,
    COUNT(*)                                                   AS items_to_restock,
    SUM(CASE WHEN stock_status = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS out_of_stock,
    SUM(CASE WHEN stock_status = 'CRITICAL'     THEN 1 ELSE 0 END) AS critical,
    ROUND(SUM(estimated_order_cost), 2)                        AS total_restock_cost
FROM restock_alerts
GROUP BY store_id
ORDER BY items_to_restock DESC;

-- 8.3 Purchase orders grouped by supplier
SELECT supplier, COUNT(*) AS line_items, SUM(suggested_order_qty) AS total_units,
       ROUND(SUM(estimated_order_cost), 2) AS po_value
FROM restock_alerts
GROUP BY supplier
ORDER BY po_value DESC;

-- Export alerts for the purchasing team
INSERT OVERWRITE LOCAL DIRECTORY 'file://${hivevar:PROJECT_DIR}/reports/restock_alerts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM restock_alerts ORDER BY store_id, stock_status, product_id;

-------------------------------------------------------------------
-- Step 9: Analyze product sales and inventory together
-------------------------------------------------------------------
-- Sales velocity per store x product over the last 30 days (June 2026)
DROP TABLE IF EXISTS stock_coverage;
CREATE TABLE stock_coverage STORED AS ORC AS
SELECT
    i.store_id,
    i.product_id,
    i.product_name,
    i.category,
    i.current_stock,
    i.reorder_level,
    COALESCE(s.units_30d, 0)                         AS units_sold_30d,
    ROUND(COALESCE(s.units_30d, 0) / 30.0, 2)        AS avg_daily_units,
    CASE WHEN COALESCE(s.units_30d, 0) = 0 THEN NULL
         ELSE ROUND(i.current_stock / (s.units_30d / 30.0), 1)
    END                                              AS days_of_stock_left,
    ROUND(i.current_stock * i.unit_cost, 2)          AS stock_value
FROM inventory i
LEFT JOIN (
    SELECT store_id, product_id, SUM(quantity) AS units_30d
    FROM sales
    WHERE transaction_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
    GROUP BY store_id, product_id
) s
ON i.store_id = s.store_id AND i.product_id = s.product_id;

-- 9.1 Stockout risk: fast sellers with less than 7 days of stock left
SELECT store_id, product_id, product_name, category, current_stock,
       avg_daily_units, days_of_stock_left
FROM stock_coverage
WHERE days_of_stock_left < 7
ORDER BY days_of_stock_left ASC, avg_daily_units DESC
LIMIT 15;

-- 9.2 Overstock: slow movers with more than 60 days of stock (capital tied up)
SELECT store_id, product_id, product_name, category, current_stock,
       avg_daily_units, days_of_stock_left, stock_value
FROM stock_coverage
WHERE days_of_stock_left > 60 OR units_sold_30d = 0
ORDER BY stock_value DESC
LIMIT 15;

-- 9.3 Category view: inventory value vs. 30-day revenue (stock turnover)
SELECT
    c.category,
    c.inventory_value,
    r.revenue_30d,
    ROUND(r.revenue_30d / c.inventory_value, 2)      AS revenue_to_stock_ratio,
    c.items_at_risk
FROM (
    SELECT category,
           ROUND(SUM(stock_value), 2) AS inventory_value,
           SUM(CASE WHEN days_of_stock_left < 7 THEN 1 ELSE 0 END) AS items_at_risk
    FROM stock_coverage
    GROUP BY category
) c
JOIN (
    SELECT category, ROUND(SUM(total_amount), 2) AS revenue_30d
    FROM sales
    WHERE transaction_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
    GROUP BY category
) r
ON c.category = r.category
ORDER BY revenue_to_stock_ratio DESC;
