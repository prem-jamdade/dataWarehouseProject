/******************************************************************************************************
Procedure Name: silver.load_silver
Purpose:
    - Loads cleansed and transformed data from the Bronze layer to the Silver layer in a Data Lakehouse.
    - Applies business rules for standardizing, deduplicating, and correcting values.
    - Includes CRM and ERP data from multiple Bronze source tables into corresponding Silver target tables.
    - Adds logging and load time tracking for each table load.

Notes:
    - Tables are truncated before inserting fresh data to maintain idempotency.
    - Uses ROW_NUMBER, CASE expressions, data type casting, and conditional logic for cleansing.
    - Load durations are printed for performance tracking.
******************************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, 
			@end_time DATETIME, 
			@batch_start_time DATETIME, 
			@batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading the silver layer'; 
		PRINT '=============================================';

		-- Load CRM data
		PRINT '---------------------------------------------';
		PRINT 'Loading the CRM files';
		PRINT '---------------------------------------------';

		-- Load Customer Info
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_create_date
		)
		SELECT 
			cust_id,
			cust_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END,
			CASE 
				WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female' 
				ELSE 'n/a'
			END,
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
		) t
		WHERE flag_last = 1;  -- Keep only latest record per customer

		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- Load Product Info
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
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
			ISNULL(prd_cost, 0),
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END,
			CAST(prd_start_dt AS DATE),
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- Load Sales Details
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> INSERTING DATA INTO: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
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
			CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END,
			CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END,
			CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END,
			-- Recalculate sales if values are null or inconsistent
			CASE WHEN SLS_SALES IS NULL OR SLS_SALES <= 0 OR SLS_SALES != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE SLS_SALES
			END,
			sls_quantity,
			-- Correct price if invalid or inconsistent
			CASE WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != ABS(sls_price) / sls_quantity
				THEN ABS(sls_price) / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- Load ERP Customer Info
		PRINT '---------------------------------------------';
		PRINT 'Loading the ERP files';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) ELSE CID END,
			CASE WHEN BDATE < '1924-01-01' OR BDATE > GETDATE() THEN NULL ELSE BDATE END,
			CASE 
				WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END
		FROM bronze.erp_CUST_AZ12;

		-- Load ERP Location Info
		PRINT '>> TRUNCATE TABLE: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT 
			REPLACE(CID, '-', ''),
			CASE 
				WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
				ELSE TRIM(CNTRY)
			END
		FROM bronze.erp_LOC_A101;

		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- Load ERP Product Categories
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT ID, CAT, SUBCAT, MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2;

		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- Finalize
		SET @batch_end_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading the bronze layer is completed';
		PRINT '- Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=======================================================';

	END TRY

	BEGIN CATCH 
		PRINT '=======================================================';
		PRINT 'ERROR OCCURRED DURING THE BRONZE LAYER LOADING';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH
END

