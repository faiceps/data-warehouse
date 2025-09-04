/*
--====================================================================
Quality Checks
--====================================================================
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

-- ==================================================================
*/



-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
-- QUALITY CHECK PROCEDURE
-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

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
