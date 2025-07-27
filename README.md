# dataWarehousePeroject
This project is designed to show how raw data from CRM and ERP systems is transformed into a proper 'Sales Data Warehouse' using different data layers and a star schema model.

## 📊 Project Overview

In this project, we have built a **data warehouse system** by moving data through multiple layers – Bronze, Silver, and Gold – to finally create a **Sales Data Mart**.  
This setup is ideal for businesses who want to understand **customer behaviour, product performance, and sales trends** in a structured way.

## 🧱 Data Layers (ETL Flow)

We follow a 
3-layer architecture:

### 🔸 Bronze Layer (Raw Data)
- Data is taken as-it-is from CRM and ERP sources.
- Tables include:
  - `crm_sales_details`
  - `crm_cust_info`
  - `crm_prd_info`
  - `erp_cust_az12`
  - `erp_loc_a101`
  - `erp_px_cat_g1v2`

### 🔹 Silver Layer (Cleaned & Processed)
- Basic cleaning, formatting, and joining happens here.
- Same tables as Bronze but improved quality.

### 🟡 Gold Layer (Data Mart)
- Final tables used for reporting and analysis.
- Tables:
  - `fact_sales` (sales transactions)
  - `dim_customers` (customer information)
  - `dim_products` (product details)

## 🔗 Data Integration

We bring together CRM and ERP systems by connecting related tables:

- Sales data from `crm_sales_details`
- Customer info from both CRM (`crm_cust_info`) and ERP (`erp_cust_az12`, `erp_loc_a101`)
- Product info from CRM (`crm_prd_info`) and ERP categories (`erp_px_cat_g1v2`)

---

## ⭐ Final Data Model (Star Schema)

The Gold Layer follows the **Star Schema**:

### 1. `fact_sales`
- Contains transactional records
- Fields like `order_date`, `quantity`, `price`, `sales_amount`

### 2. `dim_customers`
- Info like `first_name`, `last_name`, `gender`, `country`, `birthdate`, `marital_status`

### 3. `dim_products`
- Details like `product_name`, `category`, `subcategory`, `maintenance`, `cost`, `start_date`


## 🎯 Use Case

This project is useful for:
- Sales analytics and business reporting
- Understanding customer and product performance
- Building dashboards or BI tools on top of the Gold Layer

## 🛠️ Tools & Technologies

- SQL
- Data Warehouse concepts
- Star Schema Design
- (You can add more here if using tools like Databricks, Snowflake, or DBT)

## 📁 Folder Structure (Recommended)

SQL_Warehouse_Project/
│
├── data_flow.png # ETL pipeline diagram
├── data_integration.png # Source system table relationships
├── data_model.png # Final Star Schema design
├── scripts/ # SQL scripts (if any)
├── mock_data/ # Sample CSVs or SQL inserts (optional)
└── README.md # This file


This is a simple project showing end-to-end flow of building a data warehouse.  
It can be used for 'learning, interviews, college projects', or to show in your portfolio.

Made with ❤️ by Prem Jamdade
