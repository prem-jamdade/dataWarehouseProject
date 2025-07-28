/*
=============================================================
Stored Procedure: bronze.load_bronze
-------------------------------------------------------------
Purpose:
This stored procedure is responsible for loading raw data 
into the Bronze layer of a data warehouse from local CSV files.
The data comes from CRM and ERP systems and is staged into 
bronze schema tables after truncating them.

Key Features:
- Measures and logs time taken to load each dataset.
- Uses BULK INSERT for efficient data ingestion.
- Uses TRY...CATCH to handle and report errors.
=============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE 
		@start_time DATETIME,
		@end_time DATETIME,
		@batch_start_time DATETIME,
		@batch_end_time DATETIME;

	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading the bronze layer';
		PRINT '=======================================================';

		-- CRM Files Loading Block
		PRINT '-------------------------------------------------------'; 
		PRINT 'Loading the CRM files';
		PRINT '-------------------------------------------------------'; 

		-- Customer Info
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';

		-- Product Info
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';

		-- Sales Details
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';

		-- ERP Files Loading Block
		PRINT '-------------------------------------------------------'; 
		PRINT 'Loading the ERP files';
		PRINT '-------------------------------------------------------'; 

		-- ERP Customer Demographics
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>> INSERTING DATA INTO: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';
		
		-- ERP Customer Location
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT '>> INSERTING DATA INTO: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';

		-- ERP Product Categories
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATE TABLE: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>> INSERTING DATA INTO: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\91845\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------';

		-- Batch completion
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
