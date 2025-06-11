/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE TABLE dbo.file_list (
    table_name VARCHAR(200),    -- e.g. 'bronze.crm_cust_info'
    file_path  VARCHAR(500)     -- e.g. 'C:\data\crm_info.csv'
);
GO



INSERT INTO dbo.file_list VALUES
('bronze.crm_cust_info', 'C:\Users\hp\Project DW\datasets\source_crm\cust_info.csv'),
('bronze.crm_prd_info', 'C:\Users\hp\Project DW\datasets\source_crm\prd_info.csv'),
('bronze.crm_sales_details', 'C:\Users\Project DW\datasets\source_crm\sales_details.csv'),
('bronze.erp_cust_az12', 'C:\Users\hp\Project DW\datasets\source_erp\CUST_AZ12.csv'),
('bronze.erp_loc_a101', 'C:\Users\hp\Project DW\datasets\source_erp\LOC_A101.csv'),
('bronze.erp_px_cat_g1v2', 'C:\Users\Project DW\datasets\source_erp\PX_CAT_G1V2.csv')
-- Add more rows as needed
GO

TRUNCATE TABLE dbo.file_list;
GO

Select * from dbo.file_list;
GO


CREATE TABLE dbo.etl_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    run_id          UNIQUEIDENTIFIER DEFAULT NEWID(),  -- Group logs per execution batch
    layer           NVARCHAR(50),                      -- bronze / silver / gold / etc.
    schema_name     NVARCHAR(100),                     -- E.g., bronze, silver, gold
    table_name      NVARCHAR(200),                     -- E.g., crm_customers
    procedure_name  NVARCHAR(200),                     -- E.g., bronze.load_bronze
    file_path       NVARCHAR(500),                     -- File path (optional)    
    status          NVARCHAR(20),                      -- Success / Failed / Warning / Skipped
    rows_loaded     INT NULL,                          -- Optional row count    
    start_time      DATETIME NOT NULL,                 -- Start of operation
    end_time        DATETIME NULL,                     -- End of operation
    duration_sec    AS DATEDIFF(SECOND, start_time, end_time) PERSISTED, -- Auto calc    
    message         NVARCHAR(MAX),                     -- Error message or info
    created_at      DATETIME DEFAULT GETDATE()         -- Timestamp of log creation
);


GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @full_table_name NVARCHAR(200), @file_path NVARCHAR(500);
    DECLARE @schema_name NVARCHAR(100), @table_name NVARCHAR(100);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @rows_loaded INT;

    -- Generate a run_id once per procedure execution for grouping logs
    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();

    DECLARE cur CURSOR FOR
        SELECT table_name, file_path FROM dbo.file_list;

    OPEN cur;
    FETCH NEXT FROM cur INTO @full_table_name, @file_path;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @schema_name = PARSENAME(@full_table_name, 2);
            SET @table_name  = PARSENAME(@full_table_name, 1);

            SET @start_time = GETDATE();

            PRINT 'TRUNCATING: ' + @full_table_name;
            SET @sql = 'TRUNCATE TABLE ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + ';';
            EXEC sp_executesql @sql;

            PRINT 'BULK INSERT into: ' + @full_table_name;
            SET @sql = '
                BULK INSERT ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + '
                FROM ''' + @file_path + '''
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    TABLOCK
                );
            ';
            EXEC sp_executesql @sql;

            -- Get count of rows loaded
            SET @sql = 'SELECT @cnt = COUNT(*) FROM ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
            EXEC sp_executesql @sql, N'@cnt INT OUTPUT', @cnt = @rows_loaded OUTPUT;

            SET @end_time = GETDATE();

            -- Insert success log
            INSERT INTO dbo.etl_log (
                run_id, layer, schema_name, table_name, procedure_name,
                file_path, status, rows_loaded, start_time, end_time, message, created_at
            ) VALUES (
                @run_id, 'bronze', @schema_name, @table_name, 'bronze.load_bronze',
                @file_path, 'Success', @rows_loaded, @start_time, @end_time,
                'Loaded successfully', GETDATE()
            );

            PRINT '✅ Loaded ' + @full_table_name + ' with ' + CAST(@rows_loaded AS NVARCHAR) + ' rows in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        END TRY
        BEGIN CATCH
            SET @end_time = GETDATE();

            -- Insert failure log
            INSERT INTO dbo.etl_log (
                run_id, layer, schema_name, table_name, procedure_name,
                file_path, status, rows_loaded, start_time, end_time, message, created_at
            ) VALUES (
                @run_id, 'bronze', @schema_name, @table_name, 'bronze.load_bronze',
                @file_path, 'Failed', NULL, @start_time, @end_time,
                ERROR_MESSAGE(), GETDATE()
            );

            PRINT '❌ ERROR for table: ' + @full_table_name;
            PRINT ERROR_MESSAGE();
        END CATCH;

        FETCH NEXT FROM cur INTO @full_table_name, @file_path;
    END;

    CLOSE cur;
    DEALLOCATE cur;
END;


EXEC bronze.load_bronze;

select * from dbo.etl_log order by id DESC;

