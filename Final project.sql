-- Databricks notebook source

use finalprojectkamale48;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Creating  Silver Delta Part-1</h2>

-- COMMAND ----------


CREATE TABLE finalprojectkamale48.silver_warehouse_retail_sales_delta_part1 (
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
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/silver/silver_warehouse_retail_sales_delta_part-1';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Creating Silver Delta part-2</h2>

-- COMMAND ----------


CREATE TABLE finalprojectkamale48.silver_warehouse_retail_sales_delta_part2 (
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
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/silver/silver_warehouse_retail_sales_delta_part-2';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Inserting into part-1 silver delta</h2>

-- COMMAND ----------


insert into silver_warehouse_retail_sales_delta_part1
select * from bronze_warehouse_retail_sales_delta_new limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Inserting into silver delta part-2</h2>

-- COMMAND ----------


insert into silver_warehouse_retail_sales_delta_part2
select * from bronze_warehouse_retail_sales_delta_new offset 100

-- COMMAND ----------


select * from silver_warehouse_retail_sales_delta_part1 limit 5

-- COMMAND ----------


select * from silver_warehouse_retail_sales_delta_part2 limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Creating Dimensions and Facts using silver delta part 1</h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Creating Dimensions</h3>

-- COMMAND ----------


CREATE TABLE default.dim_time (
  time_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
  year INT,
  month INT
)
USING delta
OPTIONS ('header' = 'true')
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales/dim_time';

-- COMMAND ----------

DESCRIBE EXTENDED silver_warehouse_retail_sales_delta_part1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Inserting into Time Dimension</h2>

-- COMMAND ----------


INSERT INTO default.dim_time (year, month)
SELECT DISTINCT
  YEAR,
  MONTH
FROM silver_warehouse_retail_sales_delta_part1;

-- COMMAND ----------

select * from default.dim_time;

-- COMMAND ----------

describe extended default.dim_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Creating supplier dimension</h1>

-- COMMAND ----------


CREATE TABLE default.dim_supplier (
  supplier_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
  supplier VARCHAR(256)
)
USING delta
OPTIONS ('header' = 'true')
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales/dim_supplier';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Inserting into supplier dimension</h2>

-- COMMAND ----------


INSERT INTO default.dim_supplier (supplier)
SELECT DISTINCT
  supplier
FROM silver_warehouse_retail_sales_delta_part1;

-- COMMAND ----------

select * from default.dim_supplier limit 5;

-- COMMAND ----------

describe extended default.dim_supplier;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Creating Item dimension</h1>

-- COMMAND ----------


CREATE TABLE default.dim_item (
  item_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
  item_code INT,
  item_description VARCHAR(256),
  item_type VARCHAR(256)
)
USING delta
OPTIONS ('header' = 'true')
LOCATION 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales/dim_item';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Inserting into Item dimension</h2>

-- COMMAND ----------


INSERT INTO default.dim_item (item_code, item_description, item_type)
SELECT DISTINCT
  item_code, 
  item_description,
  item_type
FROM silver_warehouse_retail_sales_delta_part1;

-- COMMAND ----------

select * from default.dim_item limit 5;

-- COMMAND ----------

describe extended default.dim_item;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Creating Fact tables</h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Retailsales Fact table</h3>

-- COMMAND ----------

DESCRIBE silver_warehouse_retail_sales_delta_part1;



-- COMMAND ----------

SELECT
  ds.supplier_id,
  di.item_id,
  s.RETAIL_SALES,
  s.RETAIL_TRANSFERS,
  s.WAREHOUSE_SALES,
  dt.time_id
FROM
  silver_warehouse_retail_sales_delta_part1 s
  JOIN default.dim_supplier ds ON s.SUPPLIER = ds.supplier
  JOIN default.dim_item di ON s.ITEM_CODE = di.item_code
  JOIN default.dim_time dt ON s.YEAR = dt.year AND s.MONTH = dt.month;


-- COMMAND ----------

CREATE TABLE default.fact_retailsales
USING delta
OPTIONS (
  'header' = 'true',
  'delta.columnMapping.mode' = 'name',
  'path' = 'dbfs:/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales/fact_retailsales'
)
AS
SELECT
  ds.supplier_id,
  di.item_id,
  s.RETAIL_SALES,
  s.RETAIL_TRANSFERS,
  s.WAREHOUSE_SALES,
  dt.time_id,
  current_timestamp() AS last_updated_on
FROM
  silver_warehouse_retail_sales_delta_part1 s
  JOIN default.dim_supplier ds ON s.SUPPLIER = ds.supplier
  JOIN default.dim_item di ON s.ITEM_CODE = di.item_code
  JOIN default.dim_time dt ON s.YEAR = dt.year AND s.MONTH = dt.month;


-- COMMAND ----------

select * from default.fact_retailsales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Set Theory</h1>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Merging Dimensions and Facts</h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h3>Merging Time dimension</h3>

-- COMMAND ----------

SELECT DISTINCT YEAR, MONTH 
FROM silver_warehouse_retail_sales_delta_part2
UNION
SELECT DISTINCT YEAR, MONTH 
FROM default.dim_time;

-- COMMAND ----------

SELECT DISTINCT YEAR, MONTH 
FROM silver_warehouse_retail_sales_delta_part2
INTERSECT
SELECT DISTINCT YEAR, MONTH 
FROM default.dim_time;

-- COMMAND ----------

SELECT DISTINCT YEAR, MONTH 
FROM silver_warehouse_retail_sales_delta_part2
MINUS
SELECT DISTINCT YEAR, MONTH 
FROM default.dim_time;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_merged_time AS
SELECT DISTINCT YEAR, MONTH
FROM silver_warehouse_retail_sales_delta_part2
UNION
SELECT DISTINCT year, month
FROM default.dim_time;


-- COMMAND ----------

select * from vw_merged_time

-- COMMAND ----------

MERGE INTO default.dim_time d
USING vw_merged_time t
ON d.YEAR = t.YEAR AND d.MONTH = t.MONTH
WHEN NOT MATCHED THEN
INSERT (YEAR, MONTH)
VALUES (t.YEAR, t.MONTH);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Merging Supplier Dimension</h2>

-- COMMAND ----------

select distinct supplier  
from silver_warehouse_retail_sales_delta_part2
UNION
select  supplier from default.dim_supplier

-- COMMAND ----------

select distinct supplier  
from silver_warehouse_retail_sales_delta_part2
INTERSECT
select  supplier from default.dim_supplier

-- COMMAND ----------

select distinct supplier  
from silver_warehouse_retail_sales_delta_part2
MINUS
select  supplier from default.dim_supplier

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_merged_supplier_dim AS
SELECT DISTINCT Supplier
FROM (
    SELECT Supplier
    FROM silver_warehouse_retail_sales_delta_part2
    MINUS
    SELECT Supplier
    FROM default.dim_supplier
);

-- COMMAND ----------

select * from vw_merged_supplier_dim;

-- COMMAND ----------

MERGE INTO default.dim_supplier d
USING vw_merged_supplier_dim t
ON d.supplier = t.supplier
WHEN NOT MATCHED THEN
INSERT (supplier)
VALUES (t.supplier);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Merging Item dimension table</h2>

-- COMMAND ----------

select distinct  item_code, item_description, item_type  
from silver_warehouse_retail_sales_delta_part2
UNION
select  item_code, item_description, item_type  from default.dim_item

-- COMMAND ----------

select distinct  item_code, item_description, item_type  
from silver_warehouse_retail_sales_delta_part2
INTERSECT
select  item_code, item_description, item_type  from default.dim_item

-- COMMAND ----------

select distinct  item_code, item_description, item_type  
from silver_warehouse_retail_sales_delta_part2
MINUS
select  item_code, item_description, item_type  from default.dim_item

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_new_item_temp AS
SELECT DISTINCT item_code, item_description, item_type
FROM (
    SELECT item_code, item_description, item_type
    FROM silver_warehouse_retail_sales_delta_part2

    MINUS

    SELECT item_code, item_description, item_type
    FROM default.dim_item
);

-- COMMAND ----------

select * from vw_new_item_temp

-- COMMAND ----------

MERGE INTO default.dim_item d
USING vw_new_item_temp t
ON d.item_code = t.item_code AND d.item_description = t.item_description AND d.item_type = t.item_type
WHEN NOT MATCHED THEN
INSERT (item_code, item_description, item_type) VALUES (t.item_code, t.item_description, t.item_type);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Merging Fact retailsales table </h2>

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_tran_fact_sales_aggregated AS
SELECT
  supplier_id,
  item_id,
  time_id,
  SUM(retail_sales) AS retail_sales,
  SUM(retail_transfers) AS retail_transfers,
  SUM(warehouse_sales) AS warehouse_sales,
  MAX(last_updated_on) AS last_updated_on
FROM vw_tran_fact_sales
GROUP BY supplier_id, item_id, time_id;

-- COMMAND ----------

select * from vw_tran_fact_sales limit 3

-- COMMAND ----------

MERGE INTO default.fact_retailsales f
USING vw_tran_fact_sales_aggregated t
ON f.supplier_id = t.supplier_id
   AND f.item_id = t.item_id
   AND f.time_id = t.time_id
WHEN MATCHED THEN
  UPDATE SET
    f.retail_sales = t.retail_sales,
    f.retail_transfers = t.retail_transfers,
    f.warehouse_sales = t.warehouse_sales,
    f.last_updated_on = t.last_updated_on
WHEN NOT MATCHED THEN
  INSERT (
    supplier_id,
    item_id,
    retail_sales,
    retail_transfers,
    warehouse_sales,
    time_id,
    last_updated_on
  )
  VALUES (
    t.supplier_id,
    t.item_id,
    t.retail_sales,
    t.retail_transfers,
    t.warehouse_sales,
    t.time_id,
    t.last_updated_on
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Unit testing on Dimension/Fact tables</h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Unit testing on Time dimension</h2>

-- COMMAND ----------

SELECT year, month, COUNT(*) AS count_duplicates
FROM default.dim_time
GROUP BY year, month
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Unit test on supplier dimension</h2>

-- COMMAND ----------

SELECT supplier_id, COUNT(*) AS count
FROM default.dim_supplier
GROUP BY supplier_id
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>unit test on Item dimension</h2>

-- COMMAND ----------

SELECT item_id
FROM default.dim_item
GROUP BY item_id
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Unit test on retailsales fact</h2>

-- COMMAND ----------

SELECT *
FROM default.fact_retailsales
WHERE supplier_id IS NULL OR
      item_id IS NULL OR
      retail_sales IS NULL OR
      retail_transfers IS NULL OR
      warehouse_sales IS NULL OR
      time_id IS NULL OR
      last_updated_on IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Top Suppliers by Total Retail Sales
-- MAGIC
-- MAGIC Objective:
-- MAGIC Identify the top suppliers based on their total retail sales.
-- MAGIC
-- MAGIC Metrics:
-- MAGIC
-- MAGIC     Supplier ID
-- MAGIC     Supplier Name
-- MAGIC     Total Retail Sales
-- MAGIC
-- MAGIC Visualizations:
-- MAGIC
-- MAGIC     Bar chart showcasing the top suppliers based on total retail sales.
-- MAGIC         X-axis: Supplier
-- MAGIC         Y-axis: Total Retail Sales</h2>

-- COMMAND ----------

WITH monthly_supplier_performance AS (
  SELECT
    ds.supplier_id,
    ds.supplier AS supplier_name,
    dt.year,
    dt.month,
    SUM(fr.retail_sales) AS monthly_retail_sales,
    LAG(SUM(fr.retail_sales)) OVER (PARTITION BY ds.supplier_id ORDER BY dt.year, dt.month) AS prev_month_retail_sales
  FROM
    default.fact_retailsales fr
  JOIN
    default.dim_supplier ds ON fr.supplier_id = ds.supplier_id
  JOIN
    default.dim_time dt ON fr.time_id = dt.time_id
  GROUP BY
    ds.supplier_id,
    ds.supplier,
    dt.year,
    dt.month
)
SELECT
  supplier_id,
  supplier_name,
  year,
  month,
  monthly_retail_sales,
  ROUND(((monthly_retail_sales - prev_month_retail_sales) * 100.0) / prev_month_retail_sales, 2) AS retail_sales_change
FROM
  monthly_supplier_performance;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Objective:
-- MAGIC Identify the item category with the highest sales percentage in overall retail sales.
-- MAGIC
-- MAGIC Metrics:
-- MAGIC
-- MAGIC     Item ID
-- MAGIC     Item Description
-- MAGIC     Sales Percentage in Overall Retail Sales
-- MAGIC
-- MAGIC Visualization:
-- MAGIC
-- MAGIC A simple bar chart or table showcasing the item category with the highest sales percentage would be effective. The chart could have the item categories on the X-axis and the corresponding sales percentages on the Y-axis, highlighting the category with the highest percentage.</h2>

-- COMMAND ----------

WITH ItemSales AS (
  SELECT
    di.item_id,
    di.item_description,
    SUM(fr.retail_sales) AS total_retail_sales
  FROM
    default.fact_retailsales fr
  JOIN
    default.dim_item di ON fr.item_id = di.item_id
  GROUP BY
    di.item_id,
    di.item_description
)

SELECT
  item_id,
  item_description,
  ROUND(total_retail_sales * 100.0 / SUM(total_retail_sales) OVER (), 2) AS sales_percentage
FROM
  ItemSales
ORDER BY
  sales_percentage DESC
LIMIT 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>
-- MAGIC Analysis of Monthly Sales Percentage by Item Type:
-- MAGIC
-- MAGIC Objective:
-- MAGIC Examine the distribution of monthly retail sales percentage for each item type to identify the contribution of different item types to the overall sales.
-- MAGIC
-- MAGIC Metrics:
-- MAGIC
-- MAGIC     Year
-- MAGIC     Month
-- MAGIC     Item Type
-- MAGIC     Monthly Retail Sales Percentage
-- MAGIC
-- MAGIC Visualizations:
-- MAGIC A bar chart illustrating the monthly sales percentage for each item type.
-- MAGIC
-- MAGIC     X-axis: Item Type
-- MAGIC     Y-axis: Monthly Retail Sales Percentage</h2>

-- COMMAND ----------

WITH item_sales_percentage AS (
  SELECT
    dt.year,
    dt.month,
    di.item_type,
    SUM(fr.retail_sales) / SUM(SUM(fr.retail_sales)) OVER (PARTITION BY dt.year, dt.month) * 100 AS sales_percentage
  FROM
    default.fact_retailsales fr
  JOIN
    default.dim_time dt ON fr.time_id = dt.time_id
  JOIN
    default.dim_item di ON fr.item_id = di.item_id
  GROUP BY
    dt.year,
    dt.month,
    di.item_type
)

SELECT
  year,
  month,
  item_type,
  sales_percentage
FROM
  item_sales_percentage
ORDER BY
  year,
  month,
  sales_percentage DESC;