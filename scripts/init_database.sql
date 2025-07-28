/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    If the database already exists, it will be dropped and recreated.

    The script also creates the following schemas within the database:
      - bronze  : for raw ingested data from CRM and ERP sources
      - silver  : for cleaned and integrated data
      - gold    : for the final star schema (fact and dimension tables)

WARNING:
    Running this script will DROP the existing 'DataWarehouse' database 
    if it exists. All existing data will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master 
Go

-- Drope and recreate the database 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name  = 'DataWareHouse')
BEGIN 
	ALTER DATABASE DataWareHouse set single_User with RollBack IMMEDIATE ;
	DROP DATABASE DataWareHouse;
END;
GO

-- Create the Database  
CREATE DATABASE DataWareHouse;
Go


USE DataWareHouse;
Go

-- Creating Schema 

CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
