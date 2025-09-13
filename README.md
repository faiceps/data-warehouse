# Data Warehouse and Analytics Project

## 🌟 About Me

Hey, I'm **JunFai Kan**. I’m a Master of Data Science graduate passionate in pursuing my career in data engineering !

I get excited about opportunities to design scalable data pipelines, build analytical data models, and leverage modern cloud platforms like Azure and Fabric to deliver impactful business insights.

Let's connect on LinkedIn!

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/junfaikan/)

--- 

## 📖 Project Overview

Welcome to my **SQL Data Warehouse and Analytics Project** !

This project involves:

1. **Data Architecture**: Designing a Data Warehouse Using the Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

---

## 🏗️ Data Architecture

The data architecture for this project follows the Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

![Data Architecture](documents/dwh_project_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 🚀 Project Outline

### 🔧 Building the Data Warehouse (Data Engineering)

#### Objective:
Develop a modern data warehouse using Microsoft SQL Server to consolidate CRM and ERP sales data, enabling analytical reporting and informed decision-making.

#### Specifications:-
- **Data Sources**: Import data from two source systems (CRM and ERP) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historisation of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### 📊 BI: Analytics & Reporting (Data Analysis)

#### Objective:
Develop SQL-based analytics to deliver detailed insights into:-
- **Customer Behaviour**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

--- 

## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                             # Raw datasets used for the project.
│   ├── source_crm                        # CRM source data.
│       ├── cust_info.csv
│       ├── prd_info.csv
│       ├── sales_details.csv
│   ├── source_erp                        # ERP source data.
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       ├── PX_CAT_G1V2.csv
│
├── documents/                            # Project documentation and architecture details.
│   ├── data_catalogue.md                 # Catalog of datasets, including field descriptions and metadata.
│   ├── data_integration.png              # Draw.io file showing How CRM and ERP data sources integrate into unified entities.
│   ├── data_naming_rules.md              # Consistent naming guidelines for tables, columns, and files.
│   ├── dwh_project_architecture.png      # Draw.io file shows the Data Warehouse project's architecture.
│   ├── full_data_flow.png                # Draw.io file for the data flow diagram.
│   ├── sales_data_mart_star_schema.png   # Draw.io file for data models (star schema).
│
├── scripts/                              # SQL scripts for ETL and transformations.
│   ├── bronze/                           # Scripts for extracting and loading raw data.
│   ├── silver/                           # Scripts for cleaning and transforming data.
│   ├── gold/                             # Scripts for creating analytical models.
│
├── tests/                                # Test scripts and quality files.
│   ├── quality_checks_gold.sql           # Quality checks for the gold layer.
│   ├── quality_checks_silver.sql         # Quality checks for the silver layer.
│
├── LICENSE                               # License information for the repository.
├── README.md                             # Project overview and instructions.

```
---

Let's Begin!

---

# 📊 Data Warehouse and Analytics Project — Detailed Walkthrough  

## 📂 1. Datasets (`/datasets`)  
Two raw source data representing the CRM and ERP systems are ingested into the **Bronze layer**.  
The Bronze layer captures raw customer, product, and sales data within a unified database.
This provides the foundation for downstream ETL pipelines, enabling data lineage, traceability, and multi-source integration in the Data Warehouse.

Below are the details of each source systems that are used in this Data Warehouse Project:

### CRM (`/datasets/source_crm`)  
- `cust_info.csv`      → Customer profile details (IDs, names, demographics).  
- `prd_info.csv`       → Product master data (IDs, names, costs, categories).  
- `sales_details.csv`  → Sales transactions (order numbers, customer, product, dates, amounts).  

### ERP (`/datasets/source_erp`)  
- `CUST_AZ12.csv` → Additional customer attributes (birthdate, gender).  
- `LOC_A101.csv` → Customer location data (country mapping).  
- `PX_CAT_G1V2.csv` → Product classification (category, subcategory, maintenance).  

This leads into the creation of the first script below - the creation of a database 'DateWarehouse', and schemas 'bronze', 'silver', and 'gold'.

---

## 📂 2. Bronze Layer - Raw Ingestion (/scripts)

#### Script 2.1 Create and Initialise Database and Schemas 

Script(s): init.database.sql

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If the database exists, it is dropped and recreated. 
The script also sets up three schemas within the database named 'bronze', 'silver', and 'gold'.

**WARNING**:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution and ensure proper backups before proceeding to run this script.

```sql
-- Drop and recreate the 'DataWarehouse' database:
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create Database 'DataWarehouse':
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas bronze silver & gold:
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
```

#### Script 2.2 Define Bronze tables

Script(s): ddl_bronze.sql

Script Purpose:
This script creates tables in the 'bronze' schema, dropping existing tables if they exist.
Run this script to re-define the DDL structure of 'bronze' tables.

```sql
IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
);

IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt		INT,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT
);

IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id			INT,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid		NVARCHAR(50),
	cntry	NVARCHAR(50)
);

IF OBJECT_ID ('bronze,erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance	NVARCHAR(50)
);
```

#### Script 2.3 Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script(s): proc_load_bronze.sql

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:

  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
```sql
EXEC bronze.load_bronze;
```

```sql
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		PRINT '---------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\junfa\Downloads\SQL_course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================='
	END TRY
	BEGIN CATCH
		PRINT '=================================================================';
		PRINT 'ERROR OCCURRED DURING LOADING OF BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================================';
	END CATCH
END
```

<img width="1902" height="948" alt="image" src="https://github.com/user-attachments/assets/0a75741b-86d7-4f58-bb6b-66ed2cb2c866" />


---


---

## ✅ End-to-End Flow  

| Action                                   | Description                                                                 |
|------------------------------------------|-----------------------------------------------------------------------------|
| 1. Run `init.database.sql`                  | Create the DataWarehouse database and define schemas (`bronze`, `silver`, `gold`). |
| 2. Run `ddl_bronze.sql`                     | Define Bronze staging tables that mirror raw CSV source structures.          |
| 3. Execute `proc_load_bronze.sql`           | Bulk load CRM and ERP CSV data into Bronze tables.                          |
| 4. Run `ddl_silver.sql`                     | Define Silver tables with proper data types and metadata fields.            |
| 5. Execute `proc_load_silver.sql`           | Transform and cleanse Bronze data into standardized Silver tables.          |
| 6. Run `ddl_gold.sql`                       | Build Gold star schema views (`dim_customers`, `dim_products`, `fact_sales`).|
| 7. Run quality checks                       | Validate Silver & Gold layers with `quality_checks_silver.sql` and `quality_checks_gold.sql`. |
| 8. Query Gold views                         | Use analytics-ready views for BI dashboards and business reporting.         |
---



```sql
-- Step 2: Create Bronze tables
:run ddl_bronze.sql

-- Step 3: Load Bronze with raw CSVs
EXEC bronze.load_bronze;

-- Step 4: Create Silver tables
:run ddl_silver.sql

-- Step 5: Load Silver with transformations
EXEC silver.load_silver;

-- Step 6: Create Gold star schema views
:run ddl_gold.sql

-- Step 7: Run data quality checks
:run tests/quality_checks_silver.sql
:run tests/quality_checks_gold.sql

-- Step 8: Query analytics-ready tables
SELECT * FROM gold.fact_sales;
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
```
--- 

## 📂 2. Documents (`/documents`)  

### `data_catalogue.md`  
Defines the **Gold layer schema**:  
- `dim_customers`, `dim_products`, `fact_sales` (columns, data types, descriptions).  

### `data_naming_rules.md`  
Naming conventions for **schemas, tables, columns, and stored procedures**.  
- Bronze/Silver: retain source names.  
- Gold: adopt business-friendly names (`dim_`, `fact_`).  

### Diagrams  
- `dwh_project_architecture.png` → Medallion Architecture overview.  
- `full_data_flow.png` → ETL flow from raw datasets → Gold star schema.  
- `sales_data_mart_star_schema.png` → Star schema for analytics.  
- `data_integration.png` → CRM & ERP integration mapping.  

---

## 📂 3. Scripts (`/scripts`)  

### 🔹 Initialization  
- **`init.database.sql`**  
  - Creates the **DataWarehouse** database.  
  - Defines schemas: `bronze`, `silver`, `gold`.  

---

### 🔹 Bronze Layer (Raw Ingestion)  
- **`ddl_bronze.sql`**  
  - Creates raw tables (`bronze.crm_*`, `bronze.erp_*`).  
  - Mirrors source CSV structures.  

- **`proc_load_bronze.sql`**  
  - Bulk loads CSV data into Bronze tables.  
  - Steps: truncate → bulk insert → log load times.  

---

### 🔹 Silver Layer (Cleansed & Standardized)  
- **`ddl_silver.sql`**  
  - Creates structured Silver tables.  
  - Adds metadata columns (`dwh_create_date`).  
  - Enforces proper data types.  

- **`proc_load_silver.sql`**  
  - Transforms Bronze → Silver.  
  - Key transformations:  
    - Standardize marital status (`M` → Married, `S` → Single).  
    - Normalize gender (`F` → Female, `M` → Male).  
    - Derive product category IDs.  
    - Fix invalid or missing dates.  
    - Clean country codes (`DE` → Germany, `US` → United States).  

---

### 🔹 Gold Layer (Analytics-Ready)  
- **`ddl_gold.sql`**  
  - Creates **views** for star schema tables:  
    - `dim_customers` → demographics + location.  
    - `dim_products` → product attributes + ERP classifications.  
    - `fact_sales` → transactions linked to dimensions.  
  - Applies surrogate keys, joins Silver data, outputs clean, business-ready schema.  

---

## 📂 4. Tests (`/tests`)  

- **`quality_checks_silver.sql`**  
  Validates Silver layer:  
  - No null/duplicate keys.  
  - Standardized values (marital status, gender, product line).  
  - Valid date ranges (order < ship < due).  
  - Sales = Quantity × Price.  

- **`quality_checks_gold.sql`**  
  Validates Gold layer:  
  - Surrogate key uniqueness (`dim_customers`, `dim_products`).  
  - Referential integrity between `fact_sales` and dimensions.  

---


