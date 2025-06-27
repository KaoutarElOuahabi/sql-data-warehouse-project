# 🥉 Bronze Layer Documentation

## 1. 🎯 Purpose of the Bronze Layer

The **Bronze layer** is the initial entry point for all source data into the data warehouse. Think of it as the "landing zone"—where raw data is captured exactly as it arrives, with **no transformations or validations** applied.

### Primary Goals:

- **Load Reliability**: Designed to tolerate malformed or messy data without breaking the pipeline.
- **Traceability**: Provides a persistent, historical archive of raw data for auditing, debugging, and reprocessing.

By preserving the original state of the data, this layer ensures that nothing is lost, hidden, or silently corrected before being seen by the data team.

---

## 2. 🔍 Component Deep Dive: A File-by-File Explanation

This layer is powered by **two core scripts**, each playing a distinct role.

---

### 📄 File 1: `ddl_bronze.sql`

**What It Is:**  
This script contains the `CREATE TABLE` statements that define the Bronze schema’s structure.

#### Key Design Decisions:

- **Schema Isolation**  
  All tables are explicitly placed in the `bronze` schema to cleanly separate raw data from other layers (silver, gold).

- **Tolerant Data Typing**  
  For example, in `bronze.crm_sales_details`, date fields like `sls_order_dt` are declared as `NVARCHAR(50)` instead of `DATE`.  
  **Why?** To prevent failures when source files contain invalid or inconsistent formats—e.g., `"N/A"`, blanks, or malformed entries. These will load as-is and be cleaned in later layers.

---

### ⚙️ File 2: `proc_load_bronze.sql`

**What It Is:**  
This script defines the stored procedure `bronze.load_bronze`—the engine that ingests source files into Bronze tables.

#### 🔧 Architectural Breakdown

| 🧩 Pattern                | 💡 Purpose                                                                                                                                       |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| 🔌 Configuration-Driven   | Reads file paths and table names from `dbo.file_list`. No hardcoded logic. Configurable via `config.local.sql`.                                 |
| 🪵 Unified Logging        | Uses a shared `@RunID` and logs all actions to `dbo.etl_log`. Enables full traceability of each pipeline execution.                             |
| 🔁 Dynamic Loading        | Uses a `CURSOR` to loop through all entries in `file_list`. Supports any number of files without needing changes to the procedure code.          |
| 🧯 Robust Error Handling  | `TRY...CATCH...THROW` pattern ensures that failed loads are logged and halt execution. Prevents partial updates and protects data integrity.     |

---

## 3. 🚀 Execution Flow

This layer is **not** intended to be run manually in production.

### Instead:

- It is automatically triggered by the master pipeline script:

  ```sql
  /scripts/orchestration/master_run_pipeline.sql
