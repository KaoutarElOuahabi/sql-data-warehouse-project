/*
===============================================================================
Project:    DataWarehouse
Script:     proc_load_bronze.sql
Author:     kaoutar EL ouahabi
===============================================================================
Script Purpose:
    This stored procedure is responsible for the initial data ingestion into
    the 'bronze' schema. It dynamically loads data from a set of external
    CSV files, as defined in the 'dbo.file_list' configuration table.
===============================================================================
*/

USE DataWarehouse;
GO

-------------------------------------------------------------------------------
-- Stored Procedure: bronze.load_bronze (Enterprise Version)
-------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze
    @RunID UNIQUEIDENTIFIER  -- Input parameter from the orchestrator for unified logging.
AS
BEGIN
    SET NOCOUNT ON;

    -- Define constants for consistent logging and metadata.
    DECLARE @ProcedureName NVARCHAR(200) = 'bronze.load_bronze';
    DECLARE @OverallStartTime DATETIME = GETDATE();

    -- Log the start of the entire procedure execution for high-level monitoring.
    INSERT INTO dbo.etl_log (run_id, layer, procedure_name, status, start_time, message)
    VALUES (@RunID, 'Bronze', @ProcedureName, 'Running', @OverallStartTime, 'Bronze layer load started.');

    BEGIN TRY
        -- Declare variables for the dynamic loop (cursor).
        DECLARE @fullTableName NVARCHAR(200), @filePath NVARCHAR(500);
        DECLARE @schemaName NVARCHAR(100), @tableName NVARCHAR(100);
        DECLARE @sql NVARCHAR(MAX);
        DECLARE @loop_startTime DATETIME, @loop_endTime DATETIME;
        DECLARE @rowsLoaded INT;

        -- Use a cursor to iterate over the configuration table.
        -- This makes the procedure data-driven and scalable.
        DECLARE cur CURSOR FOR
            SELECT table_name, file_path FROM dbo.file_list;

        OPEN cur;
        FETCH NEXT FROM cur INTO @fullTableName, @filePath;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Parse schema and table names from the full name.
            SET @schemaName = PARSENAME(@fullTableName, 2);
            SET @tableName  = PARSENAME(@fullTableName, 1);
            SET @loop_startTime = GETDATE();

            -- Log the start of processing for each individual file.
            INSERT INTO dbo.etl_log (run_id, layer, schema_name, table_name, procedure_name, file_path, status, start_time, message)
            VALUES (@RunID, 'Bronze', @schemaName, @tableName, @ProcedureName, @filePath, 'Running', @loop_startTime, 'Loading file...');

            -- Use an inner TRY/CATCH to handle errors on a per-file basis.
            BEGIN TRY
                -- Step 1: Ensure the target table is empty before loading.
                SET @sql = 'TRUNCATE TABLE ' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName) + ';';
                EXEC sp_executesql @sql;

                -- Step 2: Dynamically build and execute the BULK INSERT command.
                SET @sql = '
                    BULK INSERT ' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName) + '
                    FROM ''' + @filePath + '''
                    WITH (
                        FIRSTROW = 2, -- Assumes a header row in the CSV.
                        FIELDTERMINATOR = '','',
                        ROWTERMINATOR = ''0x0a'', -- Hex code for Line Feed (\n), robust for cross-platform files (Unix/Linux/Mac).
                        TABLOCK -- A performance optimization for bulk loading.
                    );';
                EXEC sp_executesql @sql;

                SET @rowsLoaded = @@ROWCOUNT;
                SET @loop_endTime = GETDATE();

                -- Update the specific log entry for this file to 'Success'.
                UPDATE dbo.etl_log
                SET status = 'Success', end_time = @loop_endTime, rows_loaded = @rowsLoaded, message = 'Successfully loaded ' + CAST(@rowsLoaded AS NVARCHAR) + ' rows.'
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';

            END TRY
            BEGIN CATCH
                -- An error occurred for a single file. Log the specific error.
                SET @loop_endTime = GETDATE();
                UPDATE dbo.etl_log
                SET status = 'Failed', end_time = @loop_endTime, message = ERROR_MESSAGE()
                WHERE run_id = @RunID AND table_name = @tableName AND status = 'Running';

                -- CRITICAL: Re-throw the error to ensure the "fail-fast" design.
                -- This will stop the cursor and be caught by the outer CATCH block.
                THROW;
            END CATCH;

            FETCH NEXT FROM cur INTO @fullTableName, @filePath;
        END;

        CLOSE cur;
        DEALLOCATE cur;

        -- This section is reached only if all files in the loop loaded successfully.
        -- Update the main procedure log entry to 'Success'.
        UPDATE dbo.etl_log
        SET status = 'Success', end_time = GETDATE(), message = 'Bronze layer load completed successfully.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

    END TRY
    BEGIN CATCH
        -- This outer CATCH block handles the propagated error from the inner block.
        -- It updates the main procedure log entry to 'Failed'.
        UPDATE dbo.etl_log
        SET status = 'Failed', end_time = GETDATE(), message = 'Procedure failed. See inner error for details.'
        WHERE run_id = @RunID AND procedure_name = @ProcedureName AND status = 'Running';

        -- Best practice: Ensure the cursor is cleaned up in case of an error.
        IF CURSOR_STATUS('global','cur') >= -1
        BEGIN
            CLOSE cur;
            DEALLOCATE cur;
        END

        PRINT '❌ ' + @ProcedureName + ' failed and has been stopped.';
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
EXEC bronze.load_bronze @RunID = @TestRunID;

-- Query the log table to verify the results of the test run.
SELECT * FROM dbo.etl_log WHERE run_id = @TestRunID ORDER BY start_time;
GO
*/
