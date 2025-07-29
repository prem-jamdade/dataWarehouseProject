-- ===============================================================
-- View Definitions for Gold Layer: Customers, Products, and Sales
-- This script creates three views in the 'gold' schema:
--   1. dim_customers: Customer dimension with enriched demographic info.
--   2. dim_product: Product dimension with category and cost info.
--   3. dim_sales: Fact-like view linking sales to products and customers.
-- ===============================================================

-- Drop and recreate dim_customers view
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    -- If gender is not available in CRM, fallback to ERP; else mark as 'n/a'
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM 
    silver.crm_cust_info ci 
LEFT JOIN 
    silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN 
    silver.erp_loc_a101 la ON ci.cst_key = la.cid
WHERE 
    ci.cst_id IS NOT NULL;
GO

-- Drop and recreate dim_product view
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS  
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM 
    silver.crm_prd_info pn
LEFT JOIN 
    silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id 
WHERE 
    prd_end_dt IS NULL;  -- Include only currently active products
GO

-- Drop and recreate dim_sales view
IF OBJECT_ID('gold.dim_sales', 'V') IS NOT NULL
    DROP VIEW gold.dim_sales;
GO

CREATE VIEW gold.dim_sales AS
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shiping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quntity,
    sd.sls_price AS price
FROM 
    silver.crm_sales_details sd
LEFT JOIN 
    gold.dim_product pr ON sd.sls_prd_key = pr.product_number 
LEFT JOIN 
    gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;
