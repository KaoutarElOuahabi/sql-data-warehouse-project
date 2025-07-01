/*
===============================================================================
Project:    DataWarehouse
Script:     proc_load_silver.sql
Author:     [Your Name]
Date:       2025-06-27
===============================================================================
Script Purpose:
    This stored procedure transforms data from the 'bronze' schema and loads
    it into the 'silver' schema. It follows the orchestration and logging
    patterns established in the Bronze layer, including data cleansing,
    validation, and type conversion.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
    @RunID UNIQUEIDENTIFIER  -- Parameter for orchestration
AS
BEGIN
    SET NOCOUNT ON;

    -- Procedure-level constants and variables
    DECLARE @ProcedureName NVARCHAR(200) = 'silver.load_silver';
    DECLARE @OverallStartTime DATETIME = GETDATE();

    -- Log the start of the entire procedure execution
    INSERT INTO dbo.etl_log (run_id, layer, procedure_name, status, start_time, message)
    VALUES (@RunID, 'Silver', @ProcedureName, 'Running', @OverallStartTime, 'Silver layer transformation started.');

    BEGIN TRY
        -- A temporary table is used to define the execution order of transformations.
        CREATE TABLE #SilverTablesToProcess (table_name NVARCHAR(100), process_order INT);

        INSERT INTO #SilverTablesToProcess (table_name, process_order)
        VALUES
            ('crm_cust_info', 1), ('crm_prd_info', 2), ('crm_sales_details', 3),
            ('erp_cust_az12', 4), ('erp_loc_a101', 5), ('erp_px_cat_g1v2', 6);

        -- Declare cursor and loop variables
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

            -- Log start of this specific table's transformation
            INSERT INTO dbo.etl_log (run_id, layer, schema_name, table_name, procedure_name, status, start_time, message)
            VALUES (@RunID, 'Silver', 'silver', @tableName, @ProcedureName, 'Running', @loop_startTime, 'Transforming table...');

            BEGIN TRY
                -- The transformation logic for each table is encapsulated below.
                -- Each block truncates the target table before inserting the transformed data.

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
                    SELECT sls_ord_num, sls_prd_key, sls_cust_id, TRY_CONVERT(DATE, sls_order_dt), TRY_CONVERT(DATE, sls_ship_dt), TRY_CONVERT(DATE, sls_due_dt), ISNULL(sls_sales, sls_quantity * sls_price), sls_quantity, sls_price
                    FROM bronze.crm_sales_details;
                END
                ELSE IF @tableName = 'erp_cust_az12'
                BEGIN
                    TRUNCATE TABLE silver.erp_cust_az12;
                    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
                    SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END, CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END, CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' ELSE 'n/a' END
                    FROM bronze.erp_cust_az12;
                END
                ELSE IF @tableName = 'erp_loc_a101'
                BEGIN
                    TRUNCATE TABLE silver.erp_loc_a101;
                    INSERT INTO silver.erp_loc_a101 (cid, cntry)
                    SELECT REPLACE(cid, '-', ''), CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States' WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' ELSE TRIM(cntry) END
                    FROM bronze.erp_loc_a101;
                END
                ELSE IF @tableName = 'erp_px_cat_g1v2'
                BEGIN
                    TRUNCATE TABLE silver.erp_px_cat_g1v2;
                    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
                    SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;
                END

                SET @rowsLoaded = @@ROWCOUNT;
                SET @loop_endTime = GETDATE();

                -- Update the log entry for this table to 'Success'.
                UPDATE dbo.etl_log SET status = 'Success', end_time = @loop_endTime, rows_loaded = @rowsLoaded, message = 'Successfully transformed ' + CAST(@rowsLoaded AS NVARCHAR) + ' rows.'
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';

            END TRY
            BEGIN CATCH
                SET @loop_endTime = GETDATE();
                UPDATE dbo.etl_log SET status = 'Failed', end_time = @loop_endTime, message = ERROR_MESSAGE()
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';
                THROW; -- CRITICAL: Ensures the "fail-fast" behavior.
            END CATCH;

            FETCH NEXT FROM cur INTO @tableName;
        END;

        CLOSE cur; DEALLOCATE cur; DROP TABLE #SilverTablesToProcess;

        -- This section is reached only if all transformations in the loop succeed.
        UPDATE dbo.etl_log SET status = 'Success', end_time = GETDATE(), message = 'Silver layer transformation completed successfully.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

    END TRY
    BEGIN CATCH
        -- This outer block catches the error from the inner block, ensuring the procedure fails.
        UPDATE dbo.etl_log SET status = 'Failed', end_time = GETDATE(), message = 'Procedure failed. See inner error for details.'
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
