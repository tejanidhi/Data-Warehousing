-- Databricks notebook source
-- MAGIC %fs
-- MAGIC
-- MAGIC ls /mnt/dwfinalsmount

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales.csv.bz2

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("/user/hive/warehouse/fall_2023_finals.db/",True)

-- COMMAND ----------


drop database if exists fall_2023_finals cascade;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /mnt/dwfinalsmount/

-- COMMAND ----------


create database if not exists finalprojectkamale48;

-- COMMAND ----------


use finalprojectkamale48;

-- COMMAND ----------


drop table bronze_warehouse_retail_csv

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC
-- MAGIC ls dbfs:/mnt/dwfinalsmount/fall_2023_finals/
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating bronze table using the path we stored the data

-- COMMAND ----------


CREATE TABLE bronze_warehouse_retail_sales_csv
USING csv
OPTIONS (
  'path' '/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales.csv.bz2',
  'header' 'true',
  'inferSchema' 'true',
  'delimiter' ','
);

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking the table data

-- COMMAND ----------

select * from bronze_warehouse_retail_sales_csv limit 5;

-- COMMAND ----------


show create table bronze_warehouse_retail_sales_csv

-- COMMAND ----------


CREATE TABLE finalprojectkamale48.bronze_warehouse_retail_sales_delta_new (
  YEAR INT,
  MONTH INT,
  SUPPLIER STRING,
  ITEM_CODE STRING,
  ITEM_DESCRIPTION STRING,
  ITEM_TYPE STRING,
  RETAIL_SALES DOUBLE,
  RETAIL_TRANSFERS DOUBLE,
  WAREHOUSE_SALES DOUBLE
) USING delta
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/bronze_warehouse_retail_sales_delta';

-- COMMAND ----------


show create table bronze_warehouse_retail_sales_delta_new

-- COMMAND ----------

INSERT INTO finalprojectkamale48.bronze_warehouse_retail_sales_delta_new
SELECT
  YEAR,
  MONTH,
  SUPPLIER,
  `ITEM CODE`,
  `ITEM DESCRIPTION`,
  `ITEM TYPE`,
  CASE WHEN TRY_CAST(`RETAIL SALES` AS DOUBLE) IS NOT NULL THEN CAST(`RETAIL SALES` AS DOUBLE) ELSE NULL END,
  `RETAIL TRANSFERS`,
  `WAREHOUSE SALES`
FROM finalprojectkamale48.bronze_warehouse_retail_sales_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze Delta

-- COMMAND ----------

select * from finalprojectkamale48.bronze_warehouse_retail_sales_delta_new  limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Couting null values

-- COMMAND ----------


select count(*) from finalprojectkamale48.bronze_warehouse_retail_sales_delta_new

-- COMMAND ----------


use finalprojectkamale48

-- COMMAND ----------

select * from bronze_warehouse_retail_sales_delta_new limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking for null values in each column

-- COMMAND ----------


SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN YEAR IS NULL THEN 1 ELSE 0 END) AS null_year,
  SUM(CASE WHEN MONTH IS NULL THEN 1 ELSE 0 END) AS null_month,
  SUM(CASE WHEN SUPPLIER IS NULL THEN 1 ELSE 0 END) AS null_supplier,
  SUM(CASE WHEN ITEM_CODE IS NULL THEN 1 ELSE 0 END) AS null_item_code,
  SUM(CASE WHEN ITEM_DESCRIPTION IS NULL THEN 1 ELSE 0 END) AS null_item_description,
  SUM(CASE WHEN ITEM_TYPE IS NULL THEN 1 ELSE 0 END) AS null_item_type,
  SUM(CASE WHEN RETAIL_SALES IS NULL THEN 1 ELSE 0 END) AS null_retail_sales,
  SUM(CASE WHEN RETAIL_TRANSFERS IS NULL THEN 1 ELSE 0 END) AS null_retail_transfers,
  SUM(CASE WHEN WAREHOUSE_SALES IS NULL THEN 1 ELSE 0 END) AS null_warehouse_sales
FROM finalprojectkamale48.bronze_warehouse_retail_sales_delta_new;

-- COMMAND ----------


select * from bronze_warehouse_retail_sales_delta_new where RETAIL_SALES is null;

-- COMMAND ----------


update bronze_warehouse_retail_sales_delta_new set RETAIL_SALES = 0 where RETAIL_SALES is null

-- COMMAND ----------


