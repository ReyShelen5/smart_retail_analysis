-- Step 6: Sales analytics using HiveQL
USE retail_db;

-- 6.1 Total revenue (overall KPIs)
SELECT
    COUNT(*)                                              AS total_transactions,
    SUM(quantity)                                         AS total_units_sold,
    ROUND(SUM(total_amount), 2)                           AS total_revenue,
    ROUND(SUM(profit), 2)                                 AS total_profit,
    ROUND(100 * SUM(profit) / SUM(total_amount), 2)       AS profit_margin_pct,
    ROUND(AVG(total_amount), 2)                           AS avg_transaction_value
FROM sales;

-- 6.2 Category-wise revenue (with share of total revenue)
SELECT
    category,
    COUNT(*)                                                       AS transactions,
    SUM(quantity)                                                  AS units_sold,
    ROUND(SUM(total_amount), 2)                                    AS revenue,
    ROUND(SUM(profit), 2)                                          AS profit,
    ROUND(100 * SUM(profit) / SUM(total_amount), 2)                AS margin_pct,
    ROUND(100 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 2) AS revenue_share_pct
FROM sales
GROUP BY category
ORDER BY revenue DESC;

-- 6.3a Best-selling products overall (top 10 by units sold)
SELECT
    RANK() OVER (ORDER BY SUM(quantity) DESC)  AS sales_rank,
    product_id,
    product_name,
    category,
    SUM(quantity)                              AS units_sold,
    ROUND(SUM(total_amount), 2)                AS revenue,
    ROUND(SUM(profit), 2)                      AS profit
FROM sales
GROUP BY product_id, product_name, category
ORDER BY units_sold DESC
LIMIT 10;

-- 6.3b Best-selling product in each category
SELECT category, product_id, product_name, units_sold, revenue
FROM (
    SELECT
        category, product_id, product_name,
        SUM(quantity)               AS units_sold,
        ROUND(SUM(total_amount), 2) AS revenue,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(quantity) DESC) AS rn
    FROM sales
    GROUP BY category, product_id, product_name
) ranked
WHERE rn = 1
ORDER BY revenue DESC;

-- 6.4 Store-wise revenue
SELECT
    RANK() OVER (ORDER BY SUM(total_amount) DESC)  AS store_rank,
    store_id,
    store_city,
    COUNT(*)                                       AS transactions,
    ROUND(SUM(total_amount), 2)                    AS revenue,
    ROUND(SUM(profit), 2)                          AS profit,
    ROUND(AVG(total_amount), 2)                    AS avg_transaction_value
FROM sales
GROUP BY store_id, store_city
ORDER BY revenue DESC;

-- 6.5 Daily sales report (stored as a table + 7-day moving average)
DROP TABLE IF EXISTS daily_sales_report;
CREATE TABLE daily_sales_report STORED AS ORC AS
SELECT
    transaction_date,
    date_format(transaction_date, 'EEE')  AS day_of_week,
    transactions,
    units_sold,
    revenue,
    profit,
    ROUND(AVG(revenue) OVER (ORDER BY transaction_date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7day_avg
FROM (
    SELECT
        transaction_date,
        COUNT(*)                    AS transactions,
        SUM(quantity)               AS units_sold,
        ROUND(SUM(total_amount), 2) AS revenue,
        ROUND(SUM(profit), 2)       AS profit
    FROM sales
    GROUP BY transaction_date
) d;

-- Show the last 14 days
SELECT * FROM daily_sales_report
WHERE transaction_date >= DATE '2026-06-17'
ORDER BY transaction_date;

-- Export the full daily report as CSV for management
INSERT OVERWRITE LOCAL DIRECTORY 'file://${hivevar:PROJECT_DIR}/reports/daily_sales_report'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM daily_sales_report ORDER BY transaction_date;

-- 6.6 Monthly sales trend (month-over-month growth)
SELECT
    sales_month,
    transactions,
    revenue,
    profit,
    ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY sales_month))
          / LAG(revenue) OVER (ORDER BY sales_month), 2) AS mom_growth_pct
FROM (
    SELECT
        date_format(transaction_date, 'yyyy-MM') AS sales_month,
        COUNT(*)                                 AS transactions,
        ROUND(SUM(total_amount), 2)              AS revenue,
        ROUND(SUM(profit), 2)                    AS profit
    FROM sales
    GROUP BY date_format(transaction_date, 'yyyy-MM')
) m
ORDER BY sales_month;
