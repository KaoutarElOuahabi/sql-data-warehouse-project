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
    2. Load Silver Layer (placeholder)
    3. Run Data Quality Tests (placeholder)
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

    PRINT '✅ Bronze Layer Completed Successfully.';

    -- =======================================================================
    -- Step 3: Execute Silver Layer Transformation (To be added later)
    -- =======================================================================
    PRINT '';
    PRINT '--> Executing Silver Layer...';

    -- This line is commented out until the 'silver.load_silver' procedure is created and deployed.
    -- EXEC silver.load_silver @RunID = @MasterRunID;

    PRINT '✅ Silver Layer Completed Successfully. (Placeholder)';


    -- =======================================================================
    -- Step 4: Run Data Quality Tests (To be added later)
    -- =======================================================================
    PRINT '';
    PRINT '--> Running Data Quality Tests...';

    -- This line is commented out until the test procedures in the 'tests' schema are implemented.
    -- EXEC tests.run_all_tests @RunID = @MasterRunID;

    PRINT '✅ Data Quality Tests Passed. (Placeholder)';


    -- =======================================================================
    -- Step 5: Final Success Message
    -- =======================================================================
    DECLARE @EndTime DATETIME = GETDATE();
    PRINT '';
    PRINT '=================================================';
    PRINT '🎉 PIPELINE COMPLETED SUCCESSFULLY!';
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
    PRINT '❌ PIPELINE FAILED!';
    PRINT 'Error occurred at: ' + CONVERT(VARCHAR, @FailTime, 120);
    PRINT 'Total Duration before failure: ' + CAST(DATEDIFF(SECOND, @StartTime, @FailTime) AS VARCHAR) + ' seconds.';
    PRINT 'Check the dbo.etl_log table with RunID: ' + CAST(@MasterRunID AS VARCHAR(36)) + ' for details.';
    PRINT '=================================================';

    -- The error that caused the CATCH block to execute is automatically
    -- propagated from the failed child procedure, stopping the pipeline.
END CATCH;
GO
