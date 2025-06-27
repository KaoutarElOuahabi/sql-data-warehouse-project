/*
===============================================================================
Project:    DataWarehouse
Script:     ddl_bronze.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This script defines the table structures for the 'bronze' schema.
    It drops and recreates each table to ensure a clean state for development.
===============================================================================
*/

-- Ensure the script is running in the context of the correct database
USE DataWarehouse;
GO

-------------------------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Purpose: Holds raw customer profile data from the CRM system.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Purpose: Holds raw product catalog information from the CRM system.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id          INT,
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATETIME,
    prd_end_dt      DATETIME
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Purpose: Holds raw transactional sales data.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    -- Best Practice: Dates from sources are loaded as text (NVARCHAR) to
    -- prevent load failures from malformed data. They will be converted
    -- to a proper DATE type in the Silver layer.
    sls_order_dt    NVARCHAR(50),
    sls_ship_dt     NVARCHAR(50),
    sls_due_dt      NVARCHAR(50),
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Purpose: Holds raw customer location data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid     NVARCHAR(50),
    cntry   NVARCHAR(50)
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Purpose: Holds raw customer demographic data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid     NVARCHAR(50),
    bdate   DATE,
    gen     NVARCHAR(50)
);
GO

-------------------------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Purpose: Holds raw product category mapping data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id            NVARCHAR(50),
    cat           NVARCHAR(50),
    subcat        NVARCHAR(50),
    maintenance   NVARCHAR(50)
);
GO

PRINT 'Bronze layer DDL script completed successfully.';
GO
