/*
=============================================================================
Project:    DataWarehouse
Script:     01_create_database_and_schemas.sql
Author:     kaoutar EL ouahabi
=============================================================================
Script Purpose:
    This script creates the database, all necessary schemas, and all
    helper/logging tables ('etl_log', 'file_list'). It is the single
    source of truth for the database structure.

Execution Instructions:
    Run this script from a connection to the 'master' database.

WARNING:
    This script is configured for a DEVELOPMENT environment. It will permanently
    DROP and DELETE the 'DataWarehouse' database if it already exists.
    DO NOT run this in a production environment without modification.
=============================================================================
*/

-- Step 1: Connect to the master database to manage database creation
USE master;
GO

-- Step 2: Drop the existing database if it exists (for development resets)
IF DB_ID('DataWarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    PRINT 'Database "DataWarehouse" dropped.';
END
GO

-- Step 3: Create the new database
CREATE DATABASE DataWarehouse;
PRINT 'Database "DataWarehouse" created.';
GO

-- Step 4: Switch to the context of the newly created database
USE DataWarehouse;
GO

-- Step 5: Create Schemas
PRINT 'Creating schemas...';
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
CREATE SCHEMA utils;
GO
CREATE SCHEMA tests;
GO
CREATE SCHEMA orchestration;
GO
PRINT 'Schemas created successfully.';
GO

-- Step 6: Create Helper and Log Tables
PRINT 'Creating helper and log tables...';
GO

-- Create the central ETL log table
CREATE TABLE dbo.etl_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    run_id          UNIQUEIDENTIFIER NOT NULL,
    layer           NVARCHAR(50),
    schema_name     NVARCHAR(100),
    table_name      NVARCHAR(200),
    procedure_name  NVARCHAR(200),
    file_path       NVARCHAR(500),
    status          NVARCHAR(20),
    rows_loaded     INT NULL,
    start_time      DATETIME,
    end_time        DATETIME NULL,
    duration_sec    AS DATEDIFF(SECOND, start_time, end_time) PERSISTED,
    message         NVARCHAR(MAX),
    created_at      DATETIME DEFAULT GETDATE()
);
GO

-- Create an EMPTY file configuration table.
-- This table will be populated by each developer's local config script.
CREATE TABLE dbo.file_list (
    table_name VARCHAR(200),
    file_path  VARCHAR(500)
);
GO

PRINT 'Helper and log tables created successfully.';
GO

PRINT 'Initial setup script completed successfully.';
GO
