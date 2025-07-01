/*
===============================================================================
Project:    DataWarehouse
Script:     ddl_silver.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This script defines the table structures for the 'silver' schema. These
    tables hold the cleaned, transformed, and validated data from the Bronze
    layer, ready for business intelligence and analytics. The Silver layer
    is the "single source of truth" for the enterprise.
===============================================================================
*/

USE DataWarehouse;
GO

-------------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Purpose: Holds cleansed and deduplicated customer information. This table
--          serves as the conformed dimension for customers from the CRM system.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    -- Primary Key & Business Keys
    cst_id              INT NOT NULL,           -- The unique integer identifier for the customer.
    cst_key             NVARCHAR(50),           -- The business key for the customer.

    -- Customer Attributes (Transformed)
    cst_firstname       NVARCHAR(50),           -- Transformation: Leading/trailing whitespace removed from the source.
    cst_lastname        NVARCHAR(50),           -- Transformation: Leading/trailing whitespace removed from the source.
    cst_marital_status  NVARCHAR(50),           -- Transformation: Standardized codes to full text (e.g., 'S' -> 'Single').
    cst_gndr            NVARCHAR(50),           -- Transformation: Standardized codes to full text (e.g., 'F' -> 'Female').
    cst_create_date     DATE,                   -- The original creation date of the customer record.

    -- Auditing & Metadata
    dwh_create_date     DATETIME2 DEFAULT GETDATE(), -- Timestamp of when this record was created in the Silver layer.

    -- Constraints
    -- Best Practice: A Primary Key enforces uniqueness and is critical for query performance and data integrity.
    CONSTRAINT PK_silver_crm_cust_info PRIMARY KEY (cst_id)
);
GO

-------------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Purpose: Holds cleansed product information. This table acts as the conformed
--          dimension for products.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    -- Keys
    prd_id          INT NOT NULL,           -- The unique integer identifier for the product.
    cat_id          NVARCHAR(50),           -- Transformation: A new category key derived from the prd_key.
    prd_key         NVARCHAR(50),           -- The business key for the product.

    -- Product Attributes (Transformed)
    prd_nm          NVARCHAR(50),           -- The name of the product.
    prd_cost        INT,                    -- The cost of the product.
    prd_line        NVARCHAR(50),           -- Transformation: Standardized codes to full text (e.g., 'M' -> 'Mountain').
    prd_start_dt    DATE,                   -- The date this product version became active.
    prd_end_dt      DATE,                   -- Transformation: The calculated end date for Slowly Changing Dimensions (SCD).

    -- Auditing & Metadata
    dwh_create_date DATETIME2 DEFAULT GETDATE(), -- Timestamp of when this record was created in the Silver layer.

    -- Constraints
    CONSTRAINT PK_silver_crm_prd_info PRIMARY KEY (prd_id)
);
GO

-------------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Purpose: This table holds the transactional sales data. It acts as a
--          transactional fact table.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    -- Composite Primary Key
    sls_ord_num     NVARCHAR(50) NOT NULL,  -- The unique order number.
    sls_prd_key     NVARCHAR(50) NOT NULL,  -- The product key for the line item.

    -- Foreign Keys to Dimensions
    sls_cust_id     INT,                    -- Foreign key to the customer dimension.

    -- Date/Time Attributes (Corrected Data Types)
    -- Data Type Correction: Converted from NVARCHAR in Bronze to DATE for performance and correct date arithmetic.
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,

    -- Measures/Facts (Validated)
    sls_sales       INT,                    -- Transformation: Validated or recalculated sales amount.
    sls_quantity    INT,
    sls_price       INT,

    -- Auditing & Metadata
    dwh_create_date DATETIME2 DEFAULT GETDATE(), -- Timestamp of when this record was created in the Silver layer.

    -- Constraints
    -- Best Practice: Composite Primary Key enforces the uniqueness of each line item in an order.
    CONSTRAINT PK_silver_crm_sales_details PRIMARY KEY (sls_ord_num, sls_prd_key)
);
GO

-------------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Purpose: Holds cleansed location data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50) NOT NULL,
    cntry           NVARCHAR(50), -- Transformation: Standardize ('US' -> 'United States')
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_silver_erp_loc_a101 PRIMARY KEY (cid)
);
GO

-------------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Purpose: Holds cleansed demographic data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50) NOT NULL,
    bdate           DATE,
    gen             NVARCHAR(50), -- Transformation: Standardize ('MALE' -> 'Male')
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_silver_erp_cust_az12 PRIMARY KEY (cid)
);
GO

-------------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Purpose: Holds cleansed product category mapping data from the ERP system.
-------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50) NOT NULL,
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_silver_erp_px_cat_g1v2 PRIMARY KEY (id)
);
GO

PRINT 'Silver layer DDL script completed successfully.';
GO
