/*
===============================================================================
Project:    DataWarehouse
Script:     config.template.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This is a TEMPLATE file. It is used to generate a local configuration
    script ('config.local.sql') which is specific to each developer's machine.
    This pattern ensures that machine-specific file paths are never committed
    to the source control repository.

Instructions for Use:
    1. Copy this entire file.
    2. Rename the copy to 'config.local.sql'.
    3. The 'config.local.sql' file is already listed in '.gitignore' and will
       not be tracked by source control.
    4. Edit YOUR 'config.local.sql' file and update the @RootDataPath variable
       below to point to the 'datasets' folder on YOUR local machine.
    5. Run the 'config.local.sql' script once against your DataWarehouse
       database to populate the dbo.file_list table with your local paths.
===============================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- === CONFIGURE YOUR LOCAL ENVIRONMENT HERE ===
-- =============================================================================
DECLARE @RootDataPath NVARCHAR(500) = 'C:\PATH\TO\YOUR\PROJECT\datasets';
-- =============================================================================


-- This script will clear any existing paths and insert the local ones defined above.
PRINT 'Clearing existing file paths...';
TRUNCATE TABLE dbo.file_list;
GO

PRINT 'Inserting local file paths into dbo.file_list...';
INSERT INTO dbo.file_list (table_name, file_path) VALUES
('bronze.crm_cust_info',     @RootDataPath + '\source_crm\cust_info.csv'),
('bronze.crm_prd_info',      @RootDataPath + '\source_crm\prd_info.csv'),
('bronze.crm_sales_details', @RootDataPath + '\source_crm\sales_details.csv'),
('bronze.erp_cust_az12',     @RootDataPath + '\source_erp\CUST_AZ12.csv'),
('bronze.erp_loc_a101',      @RootDataPath + '\source_erp\LOC_A101.csv'),
('bronze.erp_px_cat_g1v2',    @RootDataPath + '\source_erp\PX_CAT_G1V2.csv');
GO

PRINT '✅ Local file paths configured successfully in dbo.file_list.';
GO
