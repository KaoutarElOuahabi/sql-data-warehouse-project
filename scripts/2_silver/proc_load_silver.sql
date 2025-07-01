/*
===============================================================================
Project:    DataWarehouse
Script:     proc_load_silver.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This stored procedure is the core of the transformation layer. It extracts
    data from the 'bronze' schema, applies a series of robust data cleansing,
    standardization, and validation rules, and loads the high-quality,
    business-ready data into the 'silver' schema.

Key Transformations & Best Practices:
    - Deduplication of records using ROW_NUMBER().
    - Derivation of new columns (e.g., cat_id from prd_key).
    - Calculation of Slowly Changing Dimension (SCD) end dates using LEAD().
    - Robust cleansing of text fields to remove hidden whitespace characters.
    - Standardization of categorical data (e.g., country codes, gender).
    - Safe data type conversion (e.g., NVARCHAR to DATE).
    - Use of Common Table Expressions (CTEs) for clarity in complex logic.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
    @RunID UNIQUEIDENTIFIER  -- Input parameter from the orchestrator for unified logging.
AS
BEGIN
    SET NOCOUNT ON;

    -- Procedure-level constants and variables for consistent logging.
    DECLARE @ProcedureName NVARCHAR(200) = 'silver.load_silver';
    DECLARE @OverallStartTime DATETIME = GETDATE();

    -- Log the start of the entire procedure execution for high-level monitoring.
    INSERT INTO dbo.etl_log (run_id, layer, procedure_name, status, start_time, message)
    VALUES (@RunID, 'Silver', @ProcedureName, 'Running', @OverallStartTime, 'Silver layer transformation started.');

    BEGIN TRY
        -- A temporary table defines the execution order of transformations.
        -- This is useful for managing dependencies between tables if they arise.
        CREATE TABLE #SilverTablesToProcess (table_name NVARCHAR(100), process_order INT);

        INSERT INTO #SilverTablesToProcess (table_name, process_order)
        VALUES
            ('crm_cust_info', 1), ('crm_prd_info', 2), ('crm_sales_details', 3),
            ('erp_cust_az12', 4), ('erp_loc_a101', 5), ('erp_px_cat_g1v2', 6);

        -- Declare cursor and loop variables for dynamic processing.
        DECLARE @tableName NVARCHAR(100);
        DECLARE @loop_startTime DATETIME, @loop_endTime DATETIME;
        DECLARE @rowsLoaded INT;

        DECLARE cur CURSOR FOR
            SELECT table_name FROM #SilverTablesToProcess ORDER BY process_order;

        OPEN cur;
        FETCH NEXT FROM cur INTO @tableName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @loop_startTime = GETDATE();

            -- Log the start of processing for each individual table.
            INSERT INTO dbo.etl_log (run_id, layer, schema_name, table_name, procedure_name, status, start_time, message)
            VALUES (@RunID, 'Silver', 'silver', @tableName, @ProcedureName, 'Running', @loop_startTime, 'Transforming table...');

            BEGIN TRY
                -- Each block truncates the target table and inserts the fully transformed data.
                IF @tableName = 'crm_cust_info'
                BEGIN
                    TRUNCATE TABLE silver.crm_cust_info;
                    INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
                    SELECT cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname), CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' ELSE 'n/a' END, CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' ELSE 'n/a' END, cst_create_date
                    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL) AS Deduped WHERE rn = 1;
                END
                ELSE IF @tableName = 'crm_prd_info'
                BEGIN
                    TRUNCATE TABLE silver.crm_prd_info;
                    INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
                    SELECT prd_id, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'), SUBSTRING(prd_key, 7, LEN(prd_key)), prd_nm, ISNULL(prd_cost, 0), CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring' ELSE 'n/a' END, CAST(prd_start_dt AS DATE), CAST(DATEADD(day, -1, LEAD(prd_start_dt, 1, '9999-12-31') OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)
                    FROM bronze.crm_prd_info;
                END
                ELSE IF @tableName = 'crm_sales_details'
                BEGIN
                    TRUNCATE TABLE silver.crm_sales_details;
                    INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
                    SELECT sls_ord_num, sls_prd_key, sls_cust_id, CASE WHEN sls_order_dt = '0' OR LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) END, CASE WHEN sls_ship_dt = '0' OR LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) END, CASE WHEN sls_due_dt = '0' OR LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) END, CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END, sls_quantity, CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END
                    FROM bronze.crm_sales_details;
                END
                ELSE IF @tableName = 'erp_cust_az12'
                BEGIN
                    TRUNCATE TABLE silver.erp_cust_az12;
                    -- Using a CTE for robust cleansing of the 'gen' column.
                    WITH CleansedData AS (
                        SELECT
                            cid, bdate,
                            -- First, remove all common hidden characters, then trim spaces.
                            TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS CleanedGen
                        FROM bronze.erp_cust_az12
                    )
                    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
                    SELECT
                        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
                        CASE WHEN bdate > GETDATE() OR bdate < '1924-01-01' THEN NULL ELSE bdate END,
                        CASE
                            WHEN UPPER(CleanedGen) IN ('F', 'FEMALE') THEN 'Female'
                            WHEN UPPER(CleanedGen) IN ('M', 'MALE') THEN 'Male'
                            ELSE 'n/a'
                        END
                    FROM CleansedData;
                END
                ELSE IF @tableName = 'erp_loc_a101'
                BEGIN
                    TRUNCATE TABLE silver.erp_loc_a101;
                    -- Using a CTE for robust cleansing and standardization of the country column.
                    WITH CleansedData AS (
                        SELECT
                            REPLACE(cid, '-', '') AS cid,
                            -- Create a fully cleaned version for all logic.
                            TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS CleanedCountry
                        FROM bronze.erp_loc_a101
                    )
                    INSERT INTO silver.erp_loc_a101 (cid, cntry)
                    SELECT
                        cid,
                        CASE
                            WHEN CleanedCountry IS NULL OR CleanedCountry = '' THEN 'n/a'
                            WHEN UPPER(CleanedCountry) IN ('US', 'USA') THEN 'United States'
                            WHEN UPPER(CleanedCountry) = 'DE' THEN 'Germany'
                            ELSE CleanedCountry
                        END AS FinalCountry
                    FROM CleansedData;
                END
                ELSE IF @tableName = 'erp_px_cat_g1v2'
                BEGIN
                    TRUNCATE TABLE silver.erp_px_cat_g1v2;
                    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
                    SELECT
                        id,
                        TRIM(cat),
                        TRIM(subcat),
                        TRIM(REPLACE(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))
                    FROM bronze.erp_px_cat_g1v2;
                END

                SET @rowsLoaded = @@ROWCOUNT;
                SET @loop_endTime = GETDATE();

                UPDATE dbo.etl_log SET status = 'Success', end_time = @loop_endTime, rows_loaded = @rowsLoaded, message = 'Successfully transformed ' + CAST(@rowsLoaded AS NVARCHAR) + ' rows.'
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';

            END TRY
            BEGIN CATCH
                SET @loop_endTime = GETDATE();
                UPDATE dbo.etl_log SET status = 'Failed', end_time = @loop_endTime, message = ERROR_MESSAGE()
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';
                THROW;
            END CATCH;

            FETCH NEXT FROM cur INTO @tableName;
        END;

        CLOSE cur; DEALLOCATE cur; DROP TABLE #SilverTablesToProcess;

        -- This section is reached only if all transformations in the loop succeed.
        UPDATE dbo.etl_log
        SET status = 'Success', end_time = GETDATE(), message = 'Silver layer transformation completed successfully.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

    END TRY
    BEGIN CATCH
        -- This outer block catches the error from the inner block, ensuring the procedure fails.
        UPDATE dbo.etl_log
        SET status = 'Failed', end_time = GETDATE(), message = 'Procedure failed. See inner error for details.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

        -- Best practice: Clean up temporary objects in case of an error.
        IF CURSOR_STATUS('global','cur') >= -1 BEGIN CLOSE cur; DEALLOCATE cur; END
        IF OBJECT_ID('tempdb..#SilverTablesToProcess') IS NOT NULL DROP TABLE #SilverTablesToProcess;

        PRINT '❌ ' + @ProcedureName + ' failed and has been stopped.';
        THROW; -- Re-throw the error so the calling orchestrator knows the pipeline has failed.
    END CATCH;

    SET NOCOUNT OFF;
END;
GO
