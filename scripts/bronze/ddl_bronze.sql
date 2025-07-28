IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cust_id INT,
	cust_key VARCHAR(50),
	cst_firsname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status CHAR(1),
	cst_gender CHAR(1),
	cst_create_date date
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
	prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     CHAR(10),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	SLS_SALES INT,
	sls_quatity INT,
	sls_price INT
);

IF OBJECT_ID('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50)
);

IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
	CID varchar(50),
	CNTRY varchar(50)
);

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50)
);

