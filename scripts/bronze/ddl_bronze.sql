/* 
====================================================================
Query Purpose:
This script ensures the clean creation of raw "bronze" layer staging tables 
for the CRM and ERP data sources in a data warehouse project. It first drops 
any existing versions of the tables and then recreates them with appropriate 
schemas for customer, product, and sales-related data.
====================================================================
*/

-- Drop and recreate CRM customer information table
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
	cust_id INT,
	cust_key VARCHAR(50),
	cst_firsname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status CHAR(1),  -- Expected values: M, S, etc.
	cst_gender CHAR(1),          -- Expected values: M, F, O, etc.
	cst_create_date DATE
);

-- Drop and recreate CRM product information table
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
	prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,            -- Assumes no decimal point; could be currency in smallest unit (e.g., cents)
    prd_line     CHAR(10),       -- Fixed-length code for product line
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- Drop and recreate CRM sales transaction details
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num  NVARCHAR(50),
	sls_prd_key  NVARCHAR(50),
	sls_cust_id  INT,
	sls_order_dt INT,            -- Likely a date represented as YYYYMMDD
	sls_ship_dt  INT,            -- Same as above
	sls_due_dt   INT,            -- Same as above
	SLS_SALES    INT,
	sls_quantity  INT,            
	sls_price    INT             -- Unit price at time of sale
);

-- Drop and recreate ERP customer demographic data
IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12(
	CID   VARCHAR(50),
	BDATE DATE,                 -- Birth date
	GEN   VARCHAR(50)           -- Gender
);

-- Drop and recreate ERP customer location data
IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101(
	CID    VARCHAR(50),
	CNTRY  VARCHAR(50)          -- Country of customer
);

-- Drop and recreate ERP product category and maintenance metadata
IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID           VARCHAR(50),
	CAT          VARCHAR(50),   -- Category
	SUBCAT       VARCHAR(50),   -- Subcategory
	MAINTENANCE  VARCHAR(50)   -- Maintenance
);
