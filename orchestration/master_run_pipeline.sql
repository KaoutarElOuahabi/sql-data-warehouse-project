/*
===============================================================================
Project:    DataWarehouse
Script:     master_run_pipeline.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This is the master orchestration script for the entire data warehouse ELT
    pipeline. It controls the sequence of operations, ensures transactional
    integrity by stopping on failure, and uses a unified RunID for logging.

Execution Order:
    1. Load Bronze Layer
    2. Load Silver Layer
    3. Run Data Quality Tests
    4. Load Gold Layer (placeholder)
===============================================================================
*/

USE DataWarehouse;
GO

PRINT '=================================================';
PRINT 'Starting Data Warehouse ELT Pipeline...';
PRINT '=================================================';

-- Step 1: Initialize the pipeline run
DECLARE @MasterRunID UNIQUEIDENTIFIER = NEWID();
DECLARE @StartTime DATETIME = GETDATE();

PRINT 'Generated Master Run ID: ' + CAST(@MasterRunID AS VARCHAR(36));
PRINT 'Pipeline Start Time: ' + CONVERT(VARCHAR, @StartTime, 120);

BEGIN TRY
    -- =======================================================================
    -- Step 2: Execute Bronze Layer Load
    -- =======================================================================
    PRINT '';
    PRINT '--> Executing Bronze Layer...';

    EXEC bronze.load_bronze @RunID = @MasterRunID;

    PRINT ' Bronze Layer Completed Successfully.';

    -- =======================================================================
    -- Step 3: Execute Silver Layer Transformation
    -- =======================================================================
    PRINT '';
    PRINT '--> Executing Silver Layer...';

    EXEC silver.load_silver @RunID = @MasterRunID;

    PRINT ' Silver Layer Completed Successfully.';


    -- =======================================================================
    -- Step 4: Run Data Quality Tests (Now Active)
    -- =======================================================================
    PRINT '';
    PRINT '--> Running Data Quality Tests...';

    -- This line is now active and calls the testing procedure we created.
    EXEC tests.run_silver_layer_checks @RunID = @MasterRunID;

    PRINT 'Data Quality Tests Passed.';


    -- =======================================================================
    -- Step 5: Final Success Message
    -- =======================================================================
    DECLARE @EndTime DATETIME = GETDATE();
    PRINT '';
    PRINT '=================================================';
    PRINT 'PIPELINE COMPLETED SUCCESSFULLY!';
    PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) + ' seconds.';
    PRINT '=================================================';

END TRY
BEGIN CATCH
    -- =======================================================================
    -- FAILURE HANDLING
    -- =======================================================================
    DECLARE @FailTime DATETIME = GETDATE();
    PRINT '';
    PRINT '=================================================';
    PRINT ' PIPELINE FAILED!';
    PRINT 'Error occurred at: ' + CONVERT(VARCHAR, @FailTime, 120);
    PRINT 'Total Duration before failure: ' + CAST(DATEDIFF(SECOND, @StartTime, @FailTime) AS VARCHAR) + ' seconds.';
    PRINT 'Check the dbo.etl_log table with RunID: ' + CAST(@MasterRunID AS VARCHAR(36)) + ' for details.';
    PRINT '=================================================';

    -- The THROW command from the child procedure will automatically
    -- propagate the error message here if you run this in SSMS.
END CATCH;
GO
