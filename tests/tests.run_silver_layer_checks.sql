/*
===============================================================================
Project:    DataWarehouse
Script:     proc_run_tests.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This script creates the stored procedure responsible for running a
    comprehensive suite of data quality checks on the Silver layer tables.
    This ensures the data is valid, accurate, and trustworthy before being
    used in the Gold layer. The tests herein are based on expert data
    analysis and validation rules.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE tests.run_silver_layer_checks
    @RunID UNIQUEIDENTIFIER -- Input parameter from the orchestrator for unified logging.
AS
BEGIN
    SET NOCOUNT ON;

    -- Procedure-level constants and variables
    DECLARE @ProcedureName NVARCHAR(200) = 'tests.run_silver_layer_checks';
    DECLARE @OverallStartTime DATETIME = GETDATE();
    DECLARE @TestName NVARCHAR(200);
    DECLARE @FailingRows INT;

    -- Log the start of the overall testing procedure
    INSERT INTO dbo.etl_log (run_id, layer, procedure_name, status, start_time, message)
    VALUES (@RunID, 'Tests', @ProcedureName, 'Running', @OverallStartTime, 'Starting data quality tests for Silver layer.');

    BEGIN TRY

        -- ===================================================================
        -- Test Suite: silver.crm_cust_info
        -- ===================================================================
        SET @TestName = 'PK check for silver.crm_cust_info (NULLs or Duplicates)';
        SELECT @FailingRows = COUNT(*) FROM (SELECT cst_id FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL) AS Fails;
        IF @FailingRows > 0 THROW 50001, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_cust_info', @ProcedureName, 'Success', @TestName + ' passed.');

        SET @TestName = 'Standardization check for cst_marital_status';
        SELECT @FailingRows = COUNT(*) FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a');
        IF @FailingRows > 0 THROW 50002, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_cust_info', @ProcedureName, 'Success', @TestName + ' passed.');

        -- ===================================================================
        -- Test Suite: silver.crm_prd_info
        -- ===================================================================
        SET @TestName = 'PK check for silver.crm_prd_info (NULLs or Duplicates)';
        SELECT @FailingRows = COUNT(*) FROM (SELECT prd_id FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 OR prd_id IS NULL) AS Fails;
        IF @FailingRows > 0 THROW 50003, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_prd_info', @ProcedureName, 'Success', @TestName + ' passed.');

        SET @TestName = 'Date range check for silver.crm_prd_info (end_dt < start_dt)';
        SELECT @FailingRows = COUNT(*) FROM silver.crm_prd_info WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;
        IF @FailingRows > 0 THROW 50004, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_prd_info', @ProcedureName, 'Success', @TestName + ' passed.');

        -- ===================================================================
        -- Test Suite: silver.crm_sales_details
        -- ===================================================================
        SET @TestName = 'Future date check for silver.crm_sales_details';
        SELECT @FailingRows = COUNT(*) FROM silver.crm_sales_details WHERE sls_order_dt > GETDATE();
        IF @FailingRows > 0 THROW 50005, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_sales_details', @ProcedureName, 'Success', @TestName + ' passed.');

        SET @TestName = 'Ship date validation for silver.crm_sales_details';
        SELECT @FailingRows = COUNT(*) FROM silver.crm_sales_details WHERE sls_ship_dt < sls_order_dt;
        IF @FailingRows > 0 THROW 50006, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_sales_details', @ProcedureName, 'Success', @TestName + ' passed.');

        SET @TestName = 'Sales total consistency for silver.crm_sales_details';
        SELECT @FailingRows = COUNT(*) FROM silver.crm_sales_details WHERE ABS(sls_sales - (sls_quantity * sls_price)) > 0.01;
        IF @FailingRows > 0 THROW 50007, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.crm_sales_details', @ProcedureName, 'Success', @TestName + ' passed.');

        -- ===================================================================
        -- Test Suite: silver.erp_cust_az12
        -- ===================================================================
        SET @TestName = 'Out-of-range birth dates for silver.erp_cust_az12';
        SELECT @FailingRows = COUNT(*) FROM silver.erp_cust_az12 WHERE bdate < '1924-01-01' OR bdate > GETDATE();
        IF @FailingRows > 0 THROW 50008, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.erp_cust_az12', @ProcedureName, 'Success', @TestName + ' passed.');

        SET @TestName = 'Standardization check for gen in silver.erp_cust_az12';
        SELECT @FailingRows = COUNT(*) FROM silver.erp_cust_az12 WHERE gen NOT IN ('Female', 'Male', 'n/a');
        IF @FailingRows > 0 THROW 50009, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.erp_cust_az12', @ProcedureName, 'Success', @TestName + ' passed.');

        -- ===================================================================
        -- Test Suite: silver.erp_loc_a101
        -- ===================================================================
        SET @TestName = 'Standardization check for cntry in silver.erp_loc_a101';
        SELECT @FailingRows = COUNT(*) FROM silver.erp_loc_a101 WHERE cntry NOT IN ('Australia', 'Canada', 'France', 'Germany', 'United Kingdom', 'United States', 'n/a');
        IF @FailingRows > 0 THROW 50010, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.erp_loc_a101', @ProcedureName, 'Success', @TestName + ' passed.');

        -- ===================================================================
        -- Test Suite: silver.erp_px_cat_g1v2
        -- ===================================================================
        SET @TestName = 'Standardization check for maintenance in silver.erp_px_cat_g1v2';
        SELECT @FailingRows = COUNT(*) FROM silver.erp_px_cat_g1v2 WHERE maintenance NOT IN ('Yes', 'No', 'n/a');
        IF @FailingRows > 0 THROW 50011, @TestName, 1;
        INSERT INTO dbo.etl_log (run_id, layer, table_name, procedure_name, status, message) VALUES (@RunID, 'Tests', 'silver.erp_px_cat_g1v2', @ProcedureName, 'Success', @TestName + ' passed.');


        -- If all tests passed, update the main log entry to 'Success'.
        UPDATE dbo.etl_log
        SET status = 'Success', end_time = GETDATE(), message = 'All data quality tests for Silver layer passed successfully.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

    END TRY
    BEGIN CATCH
        -- This block will catch the error thrown by a failing test.
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE() + ' | Found ' + CAST(@FailingRows AS VARCHAR) + ' failing rows.';

        -- Update the main procedure log entry to 'Failed' with the detailed message.
        UPDATE dbo.etl_log
        SET status = 'Failed', end_time = GETDATE(), message = @ErrorMessage
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

        PRINT '❌ ' + @ProcedureName + ' failed. Check logs for details.';
        THROW; -- Re-throw the error so the calling orchestrator knows the pipeline has failed.
    END CATCH;

    SET NOCOUNT OFF;
END;
GO

/*
-------------------------------------------------------------------------------
-- Usage Example (for manual testing)
-------------------------------------------------------------------------------
-- This block demonstrates how to execute the procedure manually for testing
-- purposes. An orchestrator would typically handle this process automatically.

DECLARE @TestRunID UNIQUEIDENTIFIER = NEWID();
EXEC tests.run_silver_layer_checks @RunID = @TestRunID;

-- Query the log table to verify the results of the test run.
SELECT * FROM dbo.etl_log WHERE run_id = @TestRunID ORDER BY start_time;
GO
*/
