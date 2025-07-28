-- ==========================================================
-- Script Purpose:
-- This script sets up the necessary "silver" layer tables 
-- for CRM and ERP data in a data warehouse environment.
-- It ensures existing tables are dropped if they already exist
-- and creates fresh ones with relevant schema definitions.
-- The default value for 'dwh_create_date' captures the ETL load timestamp.
-- ==========================================================

-- Drop and recreate CRM Customer Information Table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50), -- Unique customer key
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gender         NVARCHAR(50),
    cst_create_date    DATE,         -- Original creation date from source
    dwh_create_date    DATETIME2 DEFAULT GETDATE() -- ETL load timestamp
);
GO

-- Drop and recreate CRM Product Information Table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),   -- Product category ID
    prd_key         NVARCHAR(50),   -- Unique product key
    prd_nm          NVARCHAR(50),   -- Product name
    prd_cost        INT,
    prd_line        NVARCHAR(50),   -- Product line/group
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate CRM Sales Details Table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),   -- Sales order number
    sls_prd_key     NVARCHAR(50),   -- Reference to product
    sls_cust_id     INT,            -- Reference to customer
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,            -- Total sales amount
    sls_quantity    INT,
    sls_price       INT,            -- Unit price
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate ERP Country/Location Mapping Table
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),   -- Customer/Entity ID
    cntry           NVARCHAR(50),   -- Country
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate ERP Customer Demographic Table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),   -- Customer ID
    bdate           DATE,           -- Birthdate
    gen             NVARCHAR(50),   -- Gender
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate ERP Product Category Table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),   -- Product ID
    cat             NVARCHAR(50),   -- Category
    subcat          NVARCHAR(50),   -- Subcategory
    maintenance     NVARCHAR(50),   -- Maintenance or support level
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

