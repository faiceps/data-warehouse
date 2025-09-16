# Data Warehouse and Analytics Project - Part 1: Data Engineering

## Table of Contents

- [Data Warehouse and Analytics Project - Part 1: Data Engineering](#data-warehouse-and-analytics-project---part-1-data-engineering)
  - [🌟 About Me](#-about-me)
  - [📖 Project Overview](#-project-overview)
  - [🏗 Data Architecture](#-data-architecture)
  - [🚀 Project Outline](#-project-outline)
    - [🔧 Part 1: Building the Data Warehouse (Data Engineering)](#-part-1-building-the-data-warehouse-data-engineering)
    - [📊 Part 2: BI Analytics & Reporting (Data Analysis)](#-part-2-bi-analytics--reporting-data-analysis)
  - [Execution Flow](#execution-flow)
  - [📂 Repository Structure](#-repository-structure)

- [Data Warehouse and Analytics Project Part 1 : Full Project Showcase](#data-warehouse-and-analytics-project-part-1--full-project-showcase)
  - [1. Datasets (`/datasets`)](#-1-datasets-datasets)
  - [2. Bronze Layer - Raw Ingestion (/scripts)](#-2-bronze-layer---raw-ingestion-scripts)
    - [Script 2.1 Create and Initialise Database and Schemas](#script-21-create-and-initialise-database-and-schemas)
    - [Script 2.2 Define Bronze tables](#script-22-define-bronze-tables)
    - [Script 2.3 Stored Procedure: Load Bronze Layer (Source -> Bronze)](#script-23-stored-procedure-load-bronze-layer-source---bronze)
  - [3. Silver Layer](#-3-silver-layer)
    - [Data Standardisation & Transformation](#data-standardisation--transformation)
    - [Script 3.1 Create Silver Tables](#script-31-create-silver-tables)
    - [3.2 Data Transformation — Table: bronze.crm_cust_info](#321-table-bronzecrm_cust_info)
    - [3.2.2 Data Transformation — Table: bronze.crm_prd_info](#322-data-transformation---table-bronzecrm_prd_info)
    - [3.3.3 Data Transformation — Table: bronze.crm_sales_details](#333--data-transformation---table-bronzecrm_sales_details)
    - [3.3.4 Data Transformation — Table: bronze.erp_cust_az12](#334--data-transformation---table-bronzeerp_cust_az12)
    - [3.3.5 Data Transformation — Table: bronze.erp_loc_a101](#335--data-transformation---table-bronzeerp_loc_a101)
    - [3.3.6 Data Transformation — Table: bronze.erp_px_cat_g1v2](#336--data-transformation---table-bronzeerp_px_cat_g1v2)
    - [Script 3.4 Stored Procedure: Load Silver Layer (Bronze -> Silver)](#script-34-stored-procedure-load-silver-layer-bronze---silver)
    - [Script 3.5 Quality Checks - Silver Layer](#script-35-quality-checks---silver-layer)
  - [4. Gold Layer](#-4-gold-layer)
    - [Script 4.1 Create Gold Views](#script-41-create-gold-views)
    - [4.1.1 Create View: gold.dim_customers](#411-create-view-golddim_customers)
    - [4.1.2 Create View: gold.dim_products](#412-create-view-golddim_products)
    - [4.1.3 Create View: gold.fact_sales](#413-create-view-goldfact_sales)
    - [4.2 Quality Checks - Gold Layer](#42-quality-checks---gold-layer)

---

## 🌟 About Me

Hey, I'm **JunFai Kan**. I’m a Master of Data Science graduate passionate in pursuing my career in data engineering !

I get excited about opportunities to design scalable data pipelines, build analytical data models, and leverage modern cloud platforms like Azure and Fabric to deliver impactful business insights.

Let's connect on LinkedIn!

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/junfaikan/)

--- 

## 📖 Project Overview

Welcome to my **SQL Data Warehouse and Analytics Project** !

This project demonstrates an end-to-end Data Warehouse build using SQL Server, from raw data ingestion to business-ready reporting.

This project involves:

1. **Data Architecture**: Designing a Data Warehouse Using the Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

[**View Part 2 of the Analytics Project here, which showcases a dedicated analytics layer to answer real-world business questions and support advanced reporting.**](https://github.com/faiceps/dwh-analytics)

---

## 🏗 Data Architecture

Designed a SQL Server Data Warehouse using the Medallion Architecture (Bronze/Silver/Gold) for raw ingestion, data standardisation, and analytics-ready data marts.

![Data Architecture](documents/dwh_project_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested by loading CRM & ERP CSVs into staging for auditability and lineage.
2. **Silver Layer**: This layer includes data cleansing, standardisation, and normalisation processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 🚀 Project Outline

### 🔧 Part 1: Building the Data Warehouse (Data Engineering)

#### Objective:
Develop a modern data warehouse using Microsoft SQL Server to consolidate CRM and ERP sales data, enabling analytical reporting and informed decision-making.

#### Specifications:-
- **Data Sources**: Import data from two source systems (CRM and ERP) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historisation of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### 📊 Part 2: BI Analytics & Reporting (Data Analysis)

#### Objective:
Develop SQL-based analytics to deliver detailed insights into:-
- **Customer Behaviour**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

[**View Part 2 of the Analytics Project here**](https://github.com/faiceps/dwh-analytics). This repository builds on this Part 1 of Data Warehouse Project, adding a dedicated analytics layer to answer real-world business questions and support advanced reporting.

## Execution Flow:

| Action                                   | Description                                                                 |
|------------------------------------------|-----------------------------------------------------------------------------|
| 1. Run `init.database.sql`                  | Create the DataWarehouse database and define schemas for `bronze`, `silver`, `gold`. |
| 2. Run `ddl_bronze.sql`                     | Define Bronze staging tables that mirror raw CSV source structures.          |
| 3. Execute `proc_load_bronze.sql`           | Bulk load CRM and ERP CSV data into Bronze tables.                          |
| 4. Run `ddl_silver.sql`                     | Define Silver tables with proper data types and metadata fields.            |
| 5. Execute `proc_load_silver.sql`           | Transform and cleanse Bronze data into standardised Silver tables.          |
| 6. Run `ddl_gold.sql`                       | Build Gold star schema views `dim_customers`, `dim_products`, `fact_sales`.|
| 7. Run quality checks                       | Validate Silver & Gold layers with `quality_checks_silver.sql` and `quality_checks_gold.sql`. |
| 8. Query Gold views                         | Use analytics-ready views for BI dashboards and business reporting.         |
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

# Data Warehouse and Analytics Project Part 1 : Full Project Showcase

## 📂 1. Datasets (`/datasets`)  
Two raw source data representing the CRM and ERP systems are ingested into the **Bronze layer**.  
The Bronze layer captures raw customer, product, and sales data within a unified database.
This provides the foundation for downstream ETL pipelines, enabling data lineage, traceability, and multi-source integration in the Data Warehouse.

Below are the details of each source systems that are used in this Data Warehouse Project:

### CRM (`/datasets/source_crm`)  
- `cust_info.csv`      : Customer profile details (IDs, names, demographics).  
- `prd_info.csv`       : Product master data (IDs, names, costs, categories).  
- `sales_details.csv`  : Sales transactions (order numbers, customer, product, dates, amounts).  

### ERP (`/datasets/source_erp`)  
- `CUST_AZ12.csv` : Additional customer attributes (birthdate, gender).  
- `LOC_A101.csv` : Customer location data (country mapping).  
- `PX_CAT_G1V2.csv` : Product classification (category, subcategory, maintenance).  

This leads into the creation of the first script below - the creation of a database 'DateWarehouse', and schemas 'bronze', 'silver', and 'gold'.

---

## 🥉 2. Bronze Layer - Raw Ingestion (/scripts)

<img width="1169" height="827" alt="data_flow_diagram_bronze" src="https://github.com/user-attachments/assets/8205a1b7-bebb-4c84-9b6f-104cc8ad64b5" />

#### Script 2.1 Create and Initialise Database and Schemas 

Script(s): [init.database.sql](scripts/init.database.sql)

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

Script(s): [ddl_bronze.sql](scripts/ddl_bronze.sql)

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
Script(s): [proc_load_bronze.sql](scripts/bronze/proc_load_bronze.sql)

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

Once *proc_load_bronze.sql* is executed, the successful output shows the loading time and performance of each tables. Each source table is truncated and reloaded to ensure data freshness, with row counts logged for traceability.

<img width="1902" height="948" alt="image" src="https://github.com/user-attachments/assets/0a75741b-86d7-4f58-bb6b-66ed2cb2c866" />

---

## 🥈 3. Silver Layer

### Data Standardisation & Transformation

The following data transformations were performed on the Bronze layer tables prior to loading the cleaned data into the Silver layer:

#### Data Cleansing:

- Removed Duplicates from customer/product IDs.
- Handled Missing/Invalid Values in Birthdate, Gender.
- Fixed Data Types, Unwanted Characters.

#### Data Normalisation & Standardisation:

- Standardized Country Codes, Gender Values, Product Categories.

#### Business Rules & Logic:

- Validated that CRM Sales Details Table follow Business Rule: Sales = Quantity × Price .
- Validated Referential Integrity between Tables.

#### Data Enrichment:

- New Derived Columns To Enrich Tables.
- Combined CRM + ERP for Richer Customer/Product Profiles.

### Overall Business Value of Silver Layer:

- Clean, unified **customer master table** that supports segmentation, churn analysis, and personalized reporting.
- Standardised **Product Dimension** ensures consistent joins across sales transactions, enabling accurate reporting of product hierarchies, profitability, and lifecycle trends.
- **Transaction sales dataset** for accurate revenue reporting, sales forecasting, and performance analytics.
- **Customer dimension table** with demographics, enabling behavioural insights and segmentation.
- **Geographic context table** for customers, supporting regional sales analysis and market expansion insights.
- **Standardized product category hierarchy table** that enables analysis by category, subcategory, and maintenance type.
- Solid foundation for the **gold star schema**, ensuring analytics are built on consistent & reliable data.

---

#### Script 3.1 Create Silver Tables

Script(s): [ddl_silver.sql](scripts/silver/ddl_silver.sql)

Script Purpose:
This script creates tables in the 'silver' schema, dropping existing tables ifthey already exist.
Run this script to re-define the DDL structure of 'bronze' tables.

```sql
IF OBJECT_ID ('silver.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	DATE,
	sls_ship_dt		DATE,
	sls_due_dt		DATE,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id			INT,
	cat_id			NVARCHAR(50),
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATE,
	prd_end_dt		DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid				NVARCHAR(50),
	bdate			DATE,
	gen				NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid				NVARCHAR(50),
	cntry			NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id				NVARCHAR(50),
	cat				NVARCHAR(50),
	subcat			NVARCHAR(50),
	maintenance		NVARCHAR(50),
	dwh_create_date	DATETIME2 DEFAULT GETDATE()
);
```

---

### 3.2  Data Transformation

#### 3.2.1 Table: bronze.crm_cust_info

Script Purpose: 

- Performs data quality checks on the Bronze layer customer dataset to detect duplicates, NULLs, and inconsistent values.
- To solve these issues, window functions ranks records by create_date to retain only the latest entry.
- Then, TRIM() function combined with conditional logic was used to standardise gender and marital status. 

This script represents the cleansed and standardized table, which is then inserted into the Silver layer, ensuring referential integrity, consistency, and business-ready customer profiles.

```sql
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'Unknown'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'Unknown'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
;
```

#### **Full Explanation:**
```sql
-- Check for NULL or Duplicate in Primary Key
-- Expectation: No NULL or Duplicates

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;
-- Duplicates found, 3 NULLs

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT
*
FROM bronze.crm_cust_info
WHERE cst_id = 29466;
-- 3 values returned.
-- Use the LATEST create_date

---------------------------------------------
-- WINDOW FX 1: Removing Duplicate cst_id
---------------------------------------------

SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;
-- Window Function to rank & label duplicate cst_id based on create_date

SELECT
*
FROM (
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1 AND cst_id = 29466;AND cst_id = 29466;

-- Check for unwanted spaces in string values

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- TRIM() Remove leading and trailing spaces from a string

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


---------------------------------------------
-- WINDOW FX 2: Removing Unwanted Spaces
---------------------------------------------
SELECT
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
;

-- Data Standardisation & Consistency
-- Check the consistency of values in low cardinality columns

-- Checking Unique Values for Gender:
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- RULE: Use Full Abbreviation for Gender.


-----------------------------------------------------------------------
-- WINDOW FX 3: Standardise Record Names: Gender
-----------------------------------------------------------------------
SELECT
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'Unknown'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
;


-- Checking Unique Values for Marital Status:
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

---------------------------------------------------------
-- WINDOW FX 4: Standardise Record Names: Marital Status
---------------------------------------------------------
SELECT
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'Unknown'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'Unknown'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
;

--=================================================================
-- FINAL: INSERT STATEMENT
--=================================================================
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'Unknown'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	 ELSE 'Unknown'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
;
```
---
#### 3.2.2 Data Transformation - Table: bronze.crm_prd_info

Script Purpose: 

- Performed Primary Key Validation, Trims Unwanted Spaces, and Ensures Consistent Product Naming by converting abbreviations into Full Name  (example: M - Mountain, R - Road).
- String Parsing to Split Product Keys into Identifiers.
- Applied COALESCE() function to handle NULL or negative costs.
- LEAD() Window Function to Remove Overlapping Dates and Redundant Timestamps.

This script represents the cleansed, standardised product dimension table ready to enter the Silver layer, ensuring referential integrity with ERP categories and supports accurate historical reporting.

```sql
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'Unknown' 
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
;
```

#### **Full Explanation:**
```sql
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;
--------------------------------------------
-- 1. DATA QUALITY 
-- Check Primary Key for Duplicate & NULL
--------------------------------------------
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--------------------------------------------------------------------------------
-- Goal #1: JOIN id from erp_px_cat_g1v2 with prd_key from crm_prd_info
--------------------------------------------------------------------------------

-- 1. Separate prd_key into into columns:
-- into cat_id
SELECT
	prd_id,
	prd_key,
	SUBSTRING(prd_key, 1, 5) AS cat_id,		-- Extract a specific part of a string value
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

SELECT * FROM bronze.crm_prd_info

SELECT * FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2	-- Unique id
--************************************************
-- Challenge:	prd_key in crm_prd_info has '-' , whereas
--				id in erp_px_cat_g1v2 has '_'.
--************************************************
-- Solution: Replace using REPLACE(), then double-check if all unique id matched.

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		-- Replaces '-' with '_'
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN	-- Checks if every record matches
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)			-- with all unique id
-- CO_PE is not found in prd_key from table crm_prd_info.

-- 1. Separate prd_key into into columns:
-- into prd_key and to be joined between crm & erp
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info 

-- Check for sls_prd_key column in crm_sales_details:
SELECT * FROM bronze.crm_sales_details

SELECT sls_prd_key FROM bronze.crm_sales_details

--
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (		-- Matched with
SELECT sls_prd_key FROM bronze.crm_sales_details)							-- this

-----------------------------------------------------------------
-- Quality Check: Any unwanted spaces from Product Name prd_nm
-----------------------------------------------------------------
SELECT * FROM bronze.crm_prd_info

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-----------------------------------------------------------------
-- Quality Check: Check for NULL or Negative Numbers in prd_cost
-----------------------------------------------------------------
SELECT * FROM bronze.crm_prd_info

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Replace NULL with 0 using COALESCE() or ISNULL()
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

---------------------------------------
-- Data Standardisation & Consistency
---------------------------------------
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- RULE: Replace abbreviations with its full name:
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'Unknown' 
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

---------------------------------------
-- Check for Invalid Date Orders
---------------------------------------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--********************************************************************************
-- Challenge:	End Date Issue; Overlapping Start & End Date , NULL Start Date 
--********************************************************************************
-- Solution:	Derive End Date from the Start Date;
--				Use Start Date of the 'next' record as the End Date (minus 1 day)

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test	-- Solution
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
-- Solution met: Use Start Date of the 'next' record as the End Date (minus 1 day)

-- Insert LEAD() into query:
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'Unknown' 
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
FROM bronze.crm_prd_info

--********************************************************************************
-- Challenge:	Start & End Date has redundant timestamps '00:00:00.000'
--********************************************************************************
-- Solution: CAST both column as DATE.
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'Unknown' 
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

--==========================================
-- FINAL: Insert into Silver layer.
--==========================================
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'Unknown' 
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
;
```
---

### 3.3.3  Data Transformation - Table: bronze.crm_sales_details

Script Purpose: 

- Data Quality Checks by Trimming Unwanted Spaces.
- Validated Referential Integrity between sls_cust_id and cst_id, and between sls_prd_key and prd_key, Ensuring Consistent Joins across Customer and Product Tables later on.
- Addressed invalid date formats (previously stored as integers) by converting them into proper DATE types, replacing zeros with NULL, and enforcing logical sequencing between order, ship, and due dates.
- Validated the Business Rule: Sales = Quantity * Price in Table.
- Corrected NULL, zero, or negative values in sales, price, or quantity.
- Conditional Logic to Re-calculate Accurate Sales and Prices with ABS(), NULLIF(), and case expressions.

```sql
INSERT INTO silver.crm_sales_details (
	sls_ord_num,	
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
 SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            THEN sls_sales / NULLIF(sls_quantity, 0)        -- if negative, convert value to 0.
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
```

#### **Full Explanation:**
```sql
SELECT * FROM bronze.crm_sales_details

------------------------------------------------------------
-- Check for unwanted spaces in string values 
-- No results
------------------------------------------------------------
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

------------------------------------------------------------
-- Goal: To connect the sls_cust_id from crm_sales_details
--       with cst_id from crm_cust_info.
------------------------------------------------------------

------------------------------------------------------------
-- Checking Integrity: between sls_prd_key AND prd_key 
------------------------------------------------------------
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- Verification checks OK, may proceed to join the two tables.

------------------------------------------------------------
-- Checking Integrity: between sls_cust_id AND cst_id 
------------------------------------------------------------
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- Verification checks OK, may proceed to join the two tables.

------------------------------------------------------------------------------
-- Issue:      sls_order_dt, sls_ship_dt, sls_due_dt are integers, not dates.
-- Solution:   Convert data types from integers to dates.
------------------------------------------------------------------------------

-----------------------------------
-- Checking For Invalid Dates
-----------------------------------
SELECT
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
-- Multiple rows of 0 values detected.

-- Use NULLIF() function to convert from 0 to NULL:
SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8   -- "OR Not in YYYYMMDD format (8 digits)".
OR sls_order_dt > 20260101  -- Outlier Boundaries; Check for date range
OR sls_order_dt < 19000101

----------------------------------------------------------------------
-- Fix NULL & Invalid Dates in sls_order_dt
----------------------------------------------------------------------
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR)) AS DATE)          -- Convert data type firstly to VARCHAR,
    END AS sls_order_dt,                                            -- then to date (using CASTS twice).
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details

-----------------------------------------------
-- Checking For Invalid Dates sls_ship_dt
-----------------------------------------------

SELECT 
NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8   
OR sls_ship_dt > 20260101  
OR sls_ship_dt < 19000101
-- No issues with sls_ship_dt.

-- To verify & streamline the columns, 
-- apply same edits from Order Date to this column:
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    sls_ship_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details

-------------------------------------------
-- Checking For Invalid Dates sls_due_dt
-------------------------------------------
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8   
    OR sls_due_dt > 20260101  
    OR sls_due_dt < 19000101

-- To verify & streamline the columns, 
-- apply same edits from Order Date to this column:
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details

-- Order Date must always be earlier than Shipping Date or Due Date
-- Logic: Place Order -> Get Ship Date -> Get Due Date
-- Checking for invalid date (Order Date > Ship Date) , (Order Date > Due Date)
SELECT
    *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- No results found. No transformation needed.


------------------------------------------------------------------------------------------
-- BUSINESS RULE: Total Sales = Quantity * Price
-- No NULL and Negative values in sls_sales, sls_quantity, sls_price.
-- Issue:       NULL and Negative values found, Incorrect calculations .
------------------------------------------------------------------------------------------
-- Check for NULL, Negative values and 0.

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL
    OR sls_sales <= 0 
    OR sls_quantity <= 0 
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

------------------------------------------------------------------------------------------
-- BUSINESS RULE: Total Sales = Quantity * Price
-- Solutions:  
--      - If Sales = 0 , negative, or NULL, then derive it using Quantity and Price.
--      - If Price is negative or NULL, then calculate it using Sales and Quantity.
--      - If Price is negative , then convert it into a positive value.
------------------------------------------------------------------------------------------

SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            THEN sls_sales / NULLIF(sls_quantity, 0)        -- if negative, convert value to 0.
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
 
----------------------------------------------------------------------
-- Incorporate NEW sls_sales & sls_price to Fix the correct values
----------------------------------------------------------------------
 SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            THEN sls_sales / NULLIF(sls_quantity, 0)        -- if negative, convert value to 0.
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details


--================================================================
-- FINAL
--================================================================
INSERT INTO silver.crm_sales_details (
	sls_ord_num,	
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
 SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
            THEN sls_sales / NULLIF(sls_quantity, 0)        -- if negative, convert value to 0.
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
```
---
### 3.3.4  Data Transformation - Table: bronze.erp_cust_az12

Script Purpose: 

- Addressed Primary Key Inconsistencies by Removing Prefixes "NAS___" from cid to Align With cst_key from CRM Data, facilitating table joins later on.
- Applied Data Quality to bdate by Replacing Invalid Data with NULL (birth dates > 100 years old, or future dates).
- Abbreviations/Mixed Formats in gen are Standardised into Consistent Values (Female, Male, Unknown).

```sql
INSERT INTO silver.erp_cust_az12 ( 
	cid,
	bdate,
	gen
)
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'Unknown'
	END AS gen
FROM bronze.erp_cust_az12
;
```


#### **Full Explanation:**
```sql
SELECT 
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
------------------------------------------
-- GOAL: Connect cid from erp_cust_az12 
--		 to cst_key from crm_cust_info
------------------------------------------
SELECT * FROM silver.crm_cust_info

-----------------------------------------------------
-- Solution: Remove extra characters in cid "NAS___"
-----------------------------------------------------
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END cid,
	bdate,
	gen
FROM bronze.erp_cust_az12

---------------------------------------------------------
-- Check bdate: over 100 years old, future birth dates
---------------------------------------------------------
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

---------------------------------------------------------
-- Solution: Remove future bdates
---------------------------------------------------------
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate,
	gen
FROM bronze.erp_cust_az12

-----------------------------
-- Check bdates
-----------------------------
SELECT DISTINCT
	gen
FROM bronze.erp_cust_az12

---------------------------------------------------------------
-- Solution: Standardise & convert abbreviation to full name
---------------------------------------------------------------
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'Unknown'
	END AS gen
FROM bronze.erp_cust_az12;

---------------------------------------------------------
-- FINAL: Insert into silver.erp_cust_az12
---------------------------------------------------------
INSERT INTO silver.erp_cust_az12 ( 
	cid,
	bdate,
	gen
)
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'Unknown'
	END AS gen
FROM bronze.erp_cust_az12
;
```
---

### 3.3.5  Data Transformation - Table: bronze.erp_loc_a101

Script Purpose: 

- Resolved Primary Key inconsistencies by removing dashes from cid, ensuring alignment with cst_key in CRM data for accurate joins later on.
- Data Standardisation: country codes like DE are expanded to Germany, US/USA are normalized to United States, and empty or NULL values are replaced with Unknown.

```sql
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
;
```

#### **Full Explanation:**
```sql
SELECT
	cid,
	cntry
FROM bronze.erp_loc_a101

----------------------------------------------------------------------------
-- GOAL: Connect cid from crp_loc_a101 to cst_key in crm_cust_info
----------------------------------------------------------------------------
SELECT cst_key FROM silver.crm_cust_info

-------------------------------------------
-- Remove - from cid
-------------------------------------------
SELECT
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101


-------------------------------------------
-- Data Standardisation & Consistency:
-- Check all values in cntry
-------------------------------------------
SELECT DISTINCT 
	cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry

-----------------------------------------------
-- Issues to fix: 
--		- Abbreviations	DE		-> Germany
--		- Abbreviations US, USA -> United States 
--		- empty records
--		- NULL values
-----------------------------------------------
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101

---------------------------------------------------------
-- FINAL: Insert into silver.erp_loc_a101
---------------------------------------------------------
INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
;
```

---
### 3.3.6  Data Transformation - Table: bronze.erp_px_cat_g1v2

Script Purpose: 

- Aligned id field with the cat_id from crm_prd_info, (previously cleansed to resolve formatting inconsistencies).
- Data Quality Checks by Trimming Unwanted Spaces from cat, subcat, and maintenance fields to ensure Consistent Formatting.
- Low-cardinality fields were Validated for Consistency.

```sql
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
;
```

#### **Full Explanation:**
```sql
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

--------------------------------------------------
-- GOAL: Connect id from erp_px_cat_g1v2 
--		 to cat_id from crm_prd_info
-- DONE WHEN CLEANING cat_id from crm_prd_info
--------------------------------------------------

-----------------------------------
-- Checking cat for unwanted spaces
-----------------------------------
SELECT 
	* 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

SELECT 
	* 
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)

SELECT 
	* 
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

-----------------------------------------
-- Data Standardisation & Consistency
-----------------------------------------
SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2

---------------------------------------------
-- FINAL: INSERT INTO silver.erp_px_cat_g1v2
---------------------------------------------
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
;
```

---

### Script 3.4 Stored Procedure: Load Silver Layer (Bronze -> Silver)
Script(s): [proc_load_silver.sql](scripts/silver/proc_load_silver.sql)

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process
to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:

- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
```sql
EXEC silver.load_silver;
```

```sql
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	PRINT 'Loading Silver Layer';
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

	PRINT '>> -----------------------------------------------------';

	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	PRINT 'Loading CRM Tables';
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	
	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;				
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown'
		END cst_gndr,
		cst_create_date
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1
	;

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -----------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;				
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'Unknown' 
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	;

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -----------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;				
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
		sls_ord_num,	
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	 SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price <= 0 OR sls_price IS NULL 
				THEN sls_sales / NULLIF(sls_quantity, 0)        -- if negative, convert value to 0.
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
	;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -----------------------------------------------------';

	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;				
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 ( 
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			 ELSE 'Unknown'
		END AS gen
	FROM bronze.erp_cust_az12
	;
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT'>> -----------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;				
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
	)
	SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
			ELSE TRIM(cntry)
		END AS cntry
	FROM bronze.erp_loc_a101
	;
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT'>> -----------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;				
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2
	;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -----------------------------------------------------';

	SET @batch_end_time = GETDATE();
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	PRINT 'Loading Silver Layer is Completed';
	PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	END TRY
	BEGIN CATCH
		PRINT '=====================================================';
		PRINT 'ERROR OCCCURRED DURING LOADING OF SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=====================================================';
	END CATCH
END
```

---
### Script 3.5 Quality Checks - Silver Layer
Script(s): [quality_checks_silver.sql](tests/quality_checks_silver.sql)

Script Purpose:
    This script performs various quality checks for data consistency, 
    accuracy, and standardisation across the 'silver' layer. 

It includes checks for:

- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardisation and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after loading data into Silver layer.
- Investigate and resolve any discrepancies found during checks.

```sql
-- =============================================================
-- 1.	Checking table 'silver.crm_cust_info'
-- =============================================================

------------------------------------------------------------
--		* DATA QUALITY *
-- 1.1	Check for NULL or Duplicate in ** Primary Key **
--		Expectation: No NULL or Duplicates
------------------------------------------------------------
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

------------------------------------------------------------
-- 1.2	Check for unwanted spaces
--		Expectation: No results
------------------------------------------------------------
SELECT 
	cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-------------------------------------------------------------------
--		* Data Standardisation & Consistency *
-- 1.3	Check the consistency of values in low cardinality columns
--------------------------------------------------------------------
SELECT DISTINCT
	cst_marital_status
FROM silver.crm_cust_info;


-- =============================================================
-- 2.	Checking table 'silver.crm_prd_info'
-- =============================================================

------------------------------------------------------------
--		* DATA QUALITY *
-- 2.1	Check for NULL or Duplicate in ** Primary Key **
--		Expectation: No NULL or Duplicates
------------------------------------------------------------
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

------------------------------------------------------------
-- 2.2	Check for unwanted spaces
--		Expectation: No results
------------------------------------------------------------
SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

------------------------------------------------------------
-- 2.3	Check for NULL or Negative values in prd_cost
--		Expectation: No results
------------------------------------------------------------
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
;

-------------------------------------------------------------------
--		* Data Standardisation & Consistency *
-- 2.4	Check the consistency of values in low cardinality columns
--------------------------------------------------------------------
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info
;
-------------------------------------------------------------------
-- 2.5	Check for Invalid Date Orders; where Start Date > End Date
--		Expectation: No results
-------------------------------------------------------------------
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
;

-- =============================================================
-- 3.	Checking table 'silver.crm_sales_details'
-- =============================================================

------------------------------------------------------------
-- 3.1	Check for Invalid Dates
--		Expectation: No results
------------------------------------------------------------
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8   
    OR sls_due_dt > 20500101  
    OR sls_due_dt < 19000101
;

------------------------------------------------------------
-- 3.2	Check for Invalid Date Orders; where:
--          - Order Date > Shipping Date
--          - Order Date > Due Date
--
--		Expectation: No results
------------------------------------------------------------
SELECT
    *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
;

------------------------------------------------------------
--      * DATA CONSISTENCY *
-- 3.3	Check for Invalid Dates

--      BUSINESS RULE: Total Sales = Quantity * Price
--      Must not have NULL and Negative values in 
--      sls_sales, sls_quantity, sls_price.
--
--		Expectation: No results
------------------------------------------------------------
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL
    OR sls_sales <= 0 
    OR sls_quantity <= 0 
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
;

-- =============================================================
-- 4.	Checking table 'silver.erp_cust_az12'
-- =============================================================

------------------------------------------------------------
-- 4.1	Check for Invalid Birthdates; Out of Range
--		Expectation: Ranged within 1924-01-01 and Today
------------------------------------------------------------
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
	OR bdate > GETDATE()
;

----------------------------------------------------------
--		* Data Standardisation & Consistency *
--
-- 4.2  Check unique Gender
--      Expectation: Male, Female, Unknown
----------------------------------------------------------
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12
;

-- =============================================================
-- 5.	Checking table 'silver.erp_loc_a101'
-- =============================================================

------------------------------------------------------
-- 5.1	* Data Standardisation & Consistency *
--
--      Check all values in cntry
--      Expectation: No abbreviations and shortforms
------------------------------------------------------
SELECT DISTINCT 
	cntry 
FROM silver.erp_loc_a101
ORDER BY cntry
;

-- =============================================================
-- 6.	Checking table 'silver.erp_px_cat_g1v2'
-- =============================================================

------------------------------------------------------
-- 6.1  Check for unwanted spaces
--      Expectation: No results
------------------------------------------------------
SELECT 
	* 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
    OR subcat != TRIM(subcat)
    OR maintenance != TRIM(maintenance)
;

------------------------------------------------------
-- 6.2	* Data Standardisation & Consistency *
--
--      Check all values in maintenance
--      Expectation: Yes or No
------------------------------------------------------
SELECT DISTINCT
    maintenance
FROM silver.erp_px_cat_g1v2
;

```
---

## 🥇 4. Gold Layer

Next, the final gold layer is constructed:

<img width="1169" height="827" alt="data_flow_diagram_gold" src="https://github.com/user-attachments/assets/e10be8e2-c831-462c-b319-5a87aaac33b5" />


The gold CRM and ERP tables can be joined together as follows:

<img width="1081" height="711" alt="data_integration" src="https://github.com/user-attachments/assets/d6ef9efa-6e49-43ea-a336-e405309aa4a4" />

<img width="941" height="664" alt="sales_data_mart_star_schema" src="https://github.com/user-attachments/assets/8473652b-f9fa-4b20-aba4-05eea6a44e97" />


### Script 4.1 Create Gold Views
Script(s): [ddl_gold.sql](scripts/gold/ddl_gold.sql)

Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The gold layer represents the final dimension and fact tables (Star Schema).

Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.

The result is a **star schema** that powers Power BI dashboards, self-service analytics, and strategic KPIs such as sales trends, customer behaviour, product performance.

Usage:
These views can be queried directly for analytics and reporting.

```sql
----------------------------------------------------------------
-- CREATE DIMENSIONS TABLE: gold.dim_customers
----------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id							AS customer_id,
	ci.cst_key							AS customer_number,
	ci.cst_firstname					AS first_name,
	ci.cst_lastname						AS last_name,
	lo.cntry							AS country,
	ci.cst_marital_status				AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END									AS gender,
	ca.bdate							AS birthdate,
	ci.cst_create_date					AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
;
```
**After creating gold.dim_customers :**

<img width="1913" height="960" alt="image" src="https://github.com/user-attachments/assets/9764f334-c16d-4f15-ba9a-71c356a1abc8" />



```sql
----------------------------------------------------------------
-- CREATE DIMENSIONS TABLE: gold.dim_products
----------------------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, pn.prd_key)	AS product_key,
	pn.prd_id												AS product_id,
	pn.prd_key												AS product_number,
	pn.prd_nm												AS product_name,
	pn.cat_id												AS category_id,
	pc.cat													AS category,
	pc.subcat												AS subcategory,
	pc.maintenance,
	pn.prd_cost												AS product_cost,
	pn.prd_line												AS product_line,
	pn.prd_start_dt											AS product_start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;
```

**After creating gold.dim_products :**

<img width="1911" height="982" alt="image" src="https://github.com/user-attachments/assets/6f9add4e-9761-40ee-a233-a01f519bab8c" />

```sql
----------------------------------------------------------------
-- CREATE FACT TABLE: gold.fact_sales
----------------------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num				AS order_number,	-- DIMENSION KEY	
	pr.product_key,									-- DIMENSION KEY
	cu.customer_key,								-- DIMENSION KEY
	sd.sls_order_dt				AS order_date,		-- DATES
	sd.sls_ship_dt				AS shipping_date,	-- DATES
	sd.sls_due_dt				AS due_date,		-- DATES
	sd.sls_sales				AS sales_amount,	-- MEASURES
	sd.sls_quantity				AS quantity,		-- MEASURES
	sd.sls_price				AS price			-- MEASURES
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;
```


**After creating gold.fact_sales :**

<img width="1913" height="985" alt="image" src="https://github.com/user-attachments/assets/a48db228-012f-42be-8540-028fba5d2521" />

---

The creation of each gold tables are explained in further detail below:

---

### 4.1.1 Create View: gold.dim_customers	

Script Purpose:

1.	Join cst_key from silver.crm_cust_info to cid from silver.erp_cust_az12 .
2.	Join cst_key from silver.crm_cust_info to cid from silver.erp_loc_a101 .
3.	Rename columns to friendly & meaningful names using snake_case.
4.	Sort columns into logical, readable groups.
5.	Generate surrogate key using ROW_NUMBER().
6.	Create View: gold.dim_customers

```sql
CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id							AS customer_id,
	ci.cst_key							AS customer_number,
	ci.cst_firstname					AS first_name,
	ci.cst_lastname						AS last_name,
	lo.cntry							AS country,
	ci.cst_marital_status				AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END									AS gender,
	ca.bdate							AS birthdate,
	ci.cst_create_date					AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
;
```

#### **Full Explanation:**
```sql
-------------------------------------------------------------------
--		1.	Join cst_key from silver.crm_cust_info
--			to	 cid	 from silver.erp_cust_az12
-------------------------------------------------------------------
-- Listing out table silver.crm_cust_info:
SELECT
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
FROM silver.crm_cust_info
;

-- Listing out table silver.erp_cust_az12:
SELECT
	cid,
	bdate,
	gen
FROM silver.erp_cust_az12
;

----------------------------------------------------------------
-- LEFT JOIN silver.crm_cust_info & silver.erp_cust_az12
----------------------------------------------------------------
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
;

-------------------------------------------------------------------
--		2.	Join cst_key from silver.crm_cust_info
--			to	 cid	 from silver.erp_loc_a101
-------------------------------------------------------------------
-- Listing out table silver.erp_loc_a101:
SELECT
	cid,
	cntry
FROM silver.erp_loc_a101
;

----------------------------------------------------------------
-- LEFT JOIN with silver.erp_loc_a101
----------------------------------------------------------------
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	lo.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
;


----------------------------------------------------------------
-- Checking joined tables for Duplicates:
----------------------------------------------------------------
SELECT cst_id, COUNT(*) FROM (			-- GROUP BY function on subquery
	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		lo.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON		  ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 AS lo
	ON		  ci.cst_key = lo.cid
)t GROUP BY cst_id 
HAVING COUNT(*) > 1
;

----------------------------------------------------------------
-- Issue:		Two genders found in table
-- Solution:	Data Integration
----------------------------------------------------------------
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
ORDER BY 1, 2
;

---------------------------------------------------------------------------------
-- Issue:		- Gender info mismatch between CRM & ERP columns
--				- NULL in ERP
--				- Unknown in CRM
-- Question:	Which source table is the master data source for these values?
-- 
-- Solution:	CRM is the Master Source of Customer Data.
---------------------------------------------------------------------------------

----------------------------------------------------------------
-- RULE:
--		- When gender data found in CRM, then use CRM data.
--		- COALESCE() : If NULL then convert to Unknown.
--		- ELSE		 : Use gender data available on ERP data.
----------------------------------------------------------------
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		-- Use Master CRM gender data.
		ELSE COALESCE(ca.gen, 'Unknown')
	END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
ORDER BY 1, 2
;

----------------------------------------------------------------
-- Integrate CASE query
----------------------------------------------------------------
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END AS new_gen,
	ci.cst_create_date,
	ca.bdate,
	lo.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
;


----------------------------------------------------------------
-- GOAL: Rename columns to friendly & meaningful names
--		 using snake_case .
----------------------------------------------------------------
SELECT
	ci.cst_id					AS customer_id,
	ci.cst_key					AS customer_number,
	ci.cst_firstname			AS first_name,
	ci.cst_lastname				AS last_name,
	ci.cst_marital_status		AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END							AS gender,
	ci.cst_create_date			AS create_date,
	ca.bdate					AS birthdate,
	lo.cntry					AS country
FROM silver.crm_cust_info		AS ci
LEFT JOIN silver.erp_cust_az12	AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101	AS lo
ON		  ci.cst_key = lo.cid
;

----------------------------------------------------------------
-- GOAL: Sort columns into logical, readable groups
----------------------------------------------------------------
SELECT
	ci.cst_id					AS customer_id,
	ci.cst_key					AS customer_number,
	ci.cst_firstname			AS first_name,
	ci.cst_lastname				AS last_name,
	lo.cntry					AS country,
	ci.cst_marital_status		AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END							AS gender,
	ca.bdate					AS birthdate,
	ci.cst_create_date			AS create_date
FROM silver.crm_cust_info		AS ci
LEFT JOIN silver.erp_cust_az12	AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101	AS lo
ON		  ci.cst_key = lo.cid
;


--=================================================================
-- QUESTION: IS THIS NEW TABLE A DIMENSION VS FACT TABLE ?
--=================================================================

----------------------------------------------------------------
-- This new table is a DIMENSION TABLE.
-- GOAL: Generate surrogate key using ROW_NUMBER()
-- GOAL: CREATE VIEW
----------------------------------------------------------------
CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id							AS customer_id,
	ci.cst_key							AS customer_number,
	ci.cst_firstname					AS first_name,
	ci.cst_lastname						AS last_name,
	lo.cntry							AS country,
	ci.cst_marital_status				AS marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr		
		ELSE COALESCE(ca.gen, 'Unknown')
	END									AS gender,
	ca.bdate							AS birthdate,
	ci.cst_create_date					AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lo
ON		  ci.cst_key = lo.cid
;

--=================================================================
-- QUALITY CHECK GOLD TABLE
--=================================================================
-- Check Gold Table columns:
SELECT * FROM gold.dim_customers

-- Check uniqueness of gender:
SELECT DISTINCT gender FROM gold.dim_customers;

```
--- 

### 4.1.2 Create View: gold.dim_products	

Script Purpose:

1.	Join cat_id from silver.crm_prd_info to id from silver.erp_px_cat_g1v2.
2.	Check for redundant columns.
3.	Rename columns to friendly & meaningful names using snake_case.
4.	Sort columns into logical, readable groups.
5.	Generate surrogate key using ROW_NUMBER().
6.	Create View: gold.dim_products .

```sql
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, pn.prd_key)	AS product_key,
	pn.prd_id												AS product_id,
	pn.prd_key												AS product_number,
	pn.prd_nm												AS product_name,
	pn.cat_id												AS category_id,
	pc.cat													AS category,
	pc.subcat												AS subcategory,
	pc.maintenance,
	pn.prd_cost												AS product_cost,
	pn.prd_line												AS product_line,
	pn.prd_start_dt											AS product_start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;
```

#### **Full Explanation:**
```sql
-------------------------------------------------------------------
-- GOAL:	Use CURRENT up-to-date data of the products,
--			filter out the past historical data of each product.
-------------------------------------------------------------------
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt
FROM silver.crm_prd_info AS pn
WHERE prd_end_dt IS NULL		-- Filter out all historical data
;

-------------------------------------------------------------------
-- GOAL:	Join cat_id	 from silver.crm_prd_info
--			to	 id		 from silver.erp_px_cat_g1v2
-------------------------------------------------------------------
-- Show silver.erp_px_cat_g1v2 table:
SELECT * FROM silver.erp_px_cat_g1v2;


SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;

-----------------------------------------------------------------
-- QUALITY CHECK OF RESULT TABLE
-----------------------------------------------------------------
-- Check for Duplicate Records:
SELECT prd_key, COUNT(*) FROM (
	SELECT 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON		  pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1
;


-------------------------------------------------------------------
-- GOALs:	- Check for redundant columns.
--			- Sort columns into logical, readable groups.
-------------------------------------------------------------------
SELECT 
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;

-------------------------------------------------------------------
-- GOAL:	Rename columns to friendly & meaningful names
--			using snake_case .
-- GOAL:	Add surrogate key using ROW_NUMBER() .
-------------------------------------------------------------------
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, pn.prd_key)	AS product_key,
	pn.prd_id												AS product_id,
	pn.prd_key												AS product_number,
	pn.prd_nm												AS product_name,
	pn.cat_id												AS category_id,
	pc.cat													AS category,
	pc.subcat												AS subcategory,
	pc.maintenance,
	pn.prd_cost												AS product_cost,
	pn.prd_line												AS product_line,
	pn.prd_start_dt											AS product_start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;

----------------------------------------------------------------
-- GOAL: CREATE VIEW
----------------------------------------------------------------
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt, pn.prd_key)	AS product_key,
	pn.prd_id												AS product_id,
	pn.prd_key												AS product_number,
	pn.prd_nm												AS product_name,
	pn.cat_id												AS category_id,
	pc.cat													AS category,
	pc.subcat												AS subcategory,
	pc.maintenance,
	pn.prd_cost												AS product_cost,
	pn.prd_line												AS product_line,
	pn.prd_start_dt											AS product_start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON		  pn.cat_id = pc.id
WHERE prd_end_dt IS NULL
;
```
---
### 4.1.3 Create View: gold.fact_sales	

Script Purpose:

1.	JOIN two gold tables' **surrogate keys**.
2.	Join sls_prd_key from silver.crm_sales_details to product_number from gold.dim_products.
3.	Join sls_cust_id from silver.crm_sales_details to customer_id from gold.dim_customers.
4.	Rename columns to friendly & meaningful names using snake_case.
5.	Sort columns into logical, readable groups.
6.	Create View: gold.fact_sales .

```sql
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num				AS order_number,	-- DIMENSION KEY	
	pr.product_key,									-- DIMENSION KEY
	cu.customer_key,								-- DIMENSION KEY
	sd.sls_order_dt				AS order_date,		-- DATES
	sd.sls_ship_dt				AS shipping_date,	-- DATES
	sd.sls_due_dt				AS due_date,		-- DATES
	sd.sls_sales				AS sales_amount,	-- MEASURES
	sd.sls_quantity				AS quantity,		-- MEASURES
	sd.sls_price				AS price			-- MEASURES
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;
```

#### **Full Explanation:**
```sql
----------------------------------------------------------------------------------
-- MAIN GOAL: JOIN TWO GOLD TABLES' SURROGATE KEYS
----------------------------------------------------------------------------------
-------------------------------------------------------------------
-- GOALs:	- Join	sls_prd_key		from silver.crm_sales_details
--			  to	product_number	from gold.dim_products
--
--			- Join	sls_cust_id		from silver.crm_sales_details
--			  to	customer_id		from gold.dim_customers
-------------------------------------------------------------------
/*
SELECT * FROM silver.crm_sales_details
SELECT * FROM gold.dim_products
SELECT * FROM gold.dim_customers
*/

SELECT
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;

-------------------------------------------------------------------
-- GOAL:	Rename columns to friendly & meaningful names
--			using snake_case .
-------------------------------------------------------------------
SELECT
	sd.sls_ord_num				AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt				AS order_date,
	sd.sls_ship_dt				AS shipping_date,
	sd.sls_due_dt				AS due_date,
	sd.sls_sales				AS sales_amount,
	sd.sls_quantity				AS quantity,
	sd.sls_price				AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;

-------------------------------------------------------------------
-- GOALs:	- Sort columns into logical, readable groups.
-------------------------------------------------------------------
SELECT
	sd.sls_ord_num				AS order_number,	-- DIMENSION KEY	
	pr.product_key,									-- DIMENSION KEY
	cu.customer_key,								-- DIMENSION KEY
	sd.sls_order_dt				AS order_date,		-- DATES
	sd.sls_ship_dt				AS shipping_date,	-- DATES
	sd.sls_due_dt				AS due_date,		-- DATES
	sd.sls_sales				AS sales_amount,	-- MEASURES
	sd.sls_quantity				AS quantity,		-- MEASURES
	sd.sls_price				AS price			-- MEASURES
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;

----------------------------------------------------------------
-- FINAL: CREATE VIEW
----------------------------------------------------------------
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num				AS order_number,	-- DIMENSION KEY	
	pr.product_key,									-- DIMENSION KEY
	cu.customer_key,								-- DIMENSION KEY
	sd.sls_order_dt				AS order_date,		-- DATES
	sd.sls_ship_dt				AS shipping_date,	-- DATES
	sd.sls_due_dt				AS due_date,		-- DATES
	sd.sls_sales				AS sales_amount,	-- MEASURES
	sd.sls_quantity				AS quantity,		-- MEASURES
	sd.sls_price				AS price			-- MEASURES
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON		  sd.sls_cust_id = cu.customer_id
;

--=================================================================
-- QUALITY CHECK OF TABLE
--=================================================================
SELECT * FROM gold.fact_sales
SELECT * FROM gold.dim_customers

-----------------------------------------------------------------
-- Check Foreign Key Integrity (Dimensions): gold.dim_customers
-----------------------------------------------------------------
SELECT * FROM gold.fact_sales
SELECT * FROM gold.dim_customers
SELECT * FROM gold.dim_products

SELECT 
	* 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON		  f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
;
-- RESULTS: *CUSTOMER DIMENSION* table has successfully joined to the *FACT* table.

----------------------------------------------------------------------------
-- Check Foreign Key Integrity (Dimensions): Repeat for gold.dim_products
----------------------------------------------------------------------------
SELECT * FROM gold.fact_sales
SELECT * FROM gold.dim_customers

SELECT 
	* 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON		  f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
ON		  f.product_key = p.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
;
-- RESULTS: *PRODUCT DIMENSION* table has successfully joined to the *FACT* table.

```
---

### 4.2 Quality Checks - Gold Layer

Script Purpose:
This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold layer. 
    
The checks below ensure:

- Uniqueness of surrogate keys in the dimension tables.
- Referential integrity between fact and dimension tables.
- Validation of relationships in the data model for analytical purposes.

Usage Notes:

- Investigate and resolve any discrepancies found during the checks.

```sql
----------------------------------------------------------------
-- CHECK TABLE: gold.dim_customers
----------------------------------------------------------------
-- Check for Uniqueness of Surrogate Key 'customer_key' in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

----------------------------------------------------------------
-- CHECK TABLE: gold.dim_products
----------------------------------------------------------------
-- Check for Uniqueness of Surrogate Key 'product_key' in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

----------------------------------------------------------------
-- CHECK TABLE: gold.fact_sales
----------------------------------------------------------------
-- Check data model connectivity between *FACT* and *DIMENSIONS*:
SELECT 
	* 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON		  f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
ON		  f.product_key = p.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
;
```


---

![Data Architecture](documents/dwh_project_architecture.png)

---



