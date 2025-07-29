/******************************************************************************************************
Purpose:
    - Performs post-load data validation checks on the Silver Layer.
    - Detects duplicates, nulls, formatting issues, invalid values, and broken business logic.
    - Assists in ensuring data quality after ETL from the Bronze to Silver layers.
******************************************************************************************************/

-- Check for duplicate or null customer IDs
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Find customer keys with leading/trailing spaces
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Review distinct values of marital status for normalization
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- Check for duplicate or null product IDs
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Find product names with leading/trailing spaces
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Find negative or null product costs
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review distinct product line values
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Identify records with product end date earlier than start date
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Validate raw sales due dates in bronze for non-YYYYMMDD, zero or invalid values
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Find inconsistent ordering dates in silver sales details
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Find sales rows where sales != quantity * price or fields are invalid
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Identify suspicious birthdates (e.g., too old or future dates)
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Review distinct gender values for normalization
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- List distinct countries after cleansing
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- Detect untrimmed category-related fields
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Review all distinct maintenance values
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
