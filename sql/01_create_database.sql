-- Step 4: Create the Hive database
CREATE DATABASE IF NOT EXISTS retail_db
COMMENT 'Smart Retail Analytics - sales, inventory and streaming data'
WITH DBPROPERTIES ('project' = 'smart-retail-analytics');

SHOW DATABASES;
DESCRIBE DATABASE EXTENDED retail_db;
