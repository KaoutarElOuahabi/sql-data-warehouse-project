# 🥈 Silver Layer Documentation

## 1. 🎯 Purpose of the Silver Layer

The Silver layer is arguably the most critical part of the data warehouse. It serves as the **single source of truth** for the enterprise. Its primary purpose is to take the raw, often messy data from the Bronze layer and transform it into a **clean**, **validated**, **integrated**, and **trustworthy** dataset ready for analytics.

### Key Features:

- **Cleansed & Standardized**  
  Data is rigorously cleaned. This includes trimming hidden whitespace characters, standardizing categorical values (e.g., converting `'USA'`, `'US'` to `'United States'`), and handling nulls gracefully.

- **Validated**  
  The data is checked against a suite of automated data quality tests to ensure its integrity. Business rules are enforced (e.g., an order date cannot be in the future).

- **Integrated**  
  In advanced scenarios, this layer is where data from different source systems (e.g., CRM and ERP customer data) is joined to create a unified view.

- **Optimized for Queries**  
  Unlike the Bronze layer, tables in the Silver layer use proper data types (e.g., `DATE`, `INT`) and have `PRIMARY KEY`s defined to ensure query performance and data integrity.

---

## 2. 🧱 Component Deep Dive: File-by-File Explanation

This layer consists of two core script files that define its structure and logic.

### 📄 `ddl_silver.sql`

**What It Is:**  
This script contains the `CREATE TABLE` statements for every table in the Silver schema. It defines the structure of the cleansed and conformed data tables.

#### Key Design Choices:

- **Data Type Correction**  
  Columns that were loaded as `NVARCHAR` in the Bronze layer for safety (like dates) are now converted to their proper, performant data types (e.g., `DATE`).

- **Primary Key Constraints**  
  Each table has a `PRIMARY KEY` defined. This enforces entity integrity (no duplicate records) and significantly improves query performance.

- **Auditing Columns**  
  A `dwh_create_date` column is included in every table to provide an audit trail showing exactly when each record was processed and loaded into the Silver layer.

---

### ⚙️ `proc_load_silver.sql`

**What It Is:**  
This script creates the `silver.load_silver` stored procedure — the "engine" that performs the complex work of transforming data from Bronze to Silver.

#### How It Works: Architectural Breakdown

- **Configuration-Driven Execution**  
  Uses a temporary table (`#SilverTablesToProcess`) to define the order of transformations. This keeps execution organized and repeatable.

- **Robust Data Cleansing**  
  Handles real-world data quality issues using expert techniques:
  
  - **Hidden Character Removal**  
    Uses `TRIM(REPLACE(...))` patterns to remove spaces, tabs (`CHAR(9)`), and line endings (`CHAR(10)`, `CHAR(13)`).

  - **Complex Standardization**  
    Uses `CASE` statements to normalize categorical values (e.g., country codes, gender).

  - **Safe Type Conversion**  
    Converts text-based dates into the `DATE` data type using safe `CASE WHEN ... THEN CAST(...)` logic.

  - **Use of CTEs**  
    Complex transformations are broken down into readable steps using `WITH` clauses.

- **Orchestration & Logging**  
  Accepts a `@RunID` parameter and logs its progress and any errors to the central `dbo.etl_log` table.

- **Fail-Fast Error Handling**  
  Wrapped in a `TRY...CATCH` block with a `THROW` command to halt execution immediately on error, protecting the integrity of the data warehouse.

---

## 3. 🚀 How This Layer is Executed

The `silver.load_silver` procedure is executed by the master orchestration script:

```

/scripts/orchestration/master\_run\_pipeline.sql

```

Execution occurs **immediately after** the Bronze layer completes successfully, and is **followed by** the execution of the data quality tests.

---
