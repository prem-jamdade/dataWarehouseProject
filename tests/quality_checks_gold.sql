-- ===============================================================
-- Data Quality Checks for Gold Layer Views and Fact Table
-- Purpose:
--   1. Check for duplicate surrogate keys in customer and product dimensions.
--   2. Identify orphan records in the fact table (sales) with missing dimension references.
-- ===============================================================

-- Check for duplicates in customer_key in dim_customers
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;  -- Return keys appearing more than once (should be unique)

-- Check for duplicates in product_key in dim_products
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;  -- Return keys appearing more than once (should be unique)

-- Check for orphan records in fact_sales where dimension references are missing
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;  -- Identifies sales records with no matching dimension data
