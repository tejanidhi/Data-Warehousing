# Warehouse and Retail Sales Data Pipeline with Delta Lake

## Project Overview

This project processes warehouse and retail sales data using Delta Lake on Databricks. The data pipeline follows a Bronze-Silver-Gold architecture and transforms the data into dimension and fact tables for a star schema suitable for analytics. The source data is ingested from CSV files, and Delta Lake optimizations are applied for performance and reliability.

## Features

- Ingest data from CSV into Delta Lake.
- Data processing in Bronze-Silver-Gold stages.
- Creation of dimension (`dim_time`, `dim_supplier`, `dim_item`) and fact (`fact_retailsales`) tables.
- Data merging from multiple parts.
- Handling of data quality checks and incremental updates.


## Getting Started

### Prerequisites

- Databricks environment: Ensure you have access to Databricks to execute SQL queries.
- Delta Lake: Leverage Delta Lake functionality for efficient data handling.

### Step-by-Step Instructions

1. Ingest Raw Data (Bronze Table): Create a Delta table from CSV data.
    ```sql
    CREATE TABLE bronze_warehouse_retail_sales_csv
    USING csv
    OPTIONS ('path' '/mnt/dwfinalsmount/fall_2023_finals/WarehouseAndRetailSales.csv.bz2', 'header' 'true', 'inferSchema' 'true', 'delimiter' ',');
    ```

2. Create Delta Tables: Transform and store data in Delta tables for further processing.
    ```sql
    CREATE TABLE bronze_warehouse_retail_sales_delta AS SELECT ...;
    ```

3. Data Quality Checks: Validate the data for null values and completeness.
    ```sql
    SELECT COUNT(*) AS total_rows, ...;
    ```

4. Build Silver Tables: Split data into two parts and load into Silver tables.
    ```sql
    INSERT INTO silver_warehouse_retail_sales_delta_part1 SELECT * FROM bronze_warehouse_retail_sales_delta LIMIT 100;
    ```

5. Create Dimension Tables: Generate `dim_time`, `dim_supplier`, and `dim_item` from Silver data.
    ```sql
    CREATE TABLE dim_time ...;
    ```

6. Build Fact Table: Join dimension and Silver data into the `fact_retailsales` table.
    ```sql
    CREATE TABLE fact_retailsales AS SELECT ...;
    ```

7. Merge Additional Data: Merge additional parts into the fact and dimension tables.
    ```sql
    MERGE INTO fact_retailsales ...;
    ```

## Additional Information

- Data Sources: Warehouse and retail sales data is sourced from CSV files.
- Architecture: The pipeline follows a multi-stage processing model (Bronze-Silver-Gold) for incremental data ingestion and cleaning.




