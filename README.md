# sql-data-warehouse-project
Building a model data warehouse with SQL Server, Including ETL Processes, data modeling and analytics 
# 📦 Enterprise SQL Data Warehouse: A Blueprint for Scalable Analytics

## 1. 🎯 Project Goal

This repository provides a production-grade SQL Server data warehouse designed to be more than just a data dump it's an engineered system built to be the *single source of truth* for enterprise analytics.

The primary objective is to offer a reference architecture that demonstrates **best practices** in enterprise data warehousing: from ingestion and transformation to orchestration, governance, and operational integrity. Built for scale and designed for clarity, this project can serve both as a blueprint and as a starting point for real-world deployments.

---

## 2. 🧠 Architectural Philosophy & Best Practices

This project is shaped by modern data engineering principles to support long-term agility, robustness, and team collaboration.

### 🔑 Core Pillars

* **Scalability & Performance**
  The system is built to handle growing data volumes and complex analytical queries with ease. The architecture prioritizes reliability in the early layers and speeds up access in the refined layers through proper indexing, data typing, and aggregation strategies.

* **Maintainability & Collaboration**
  A clean directory structure, modular SQL, and separated configuration files ensure the codebase is easy to maintain and built for teamwork.

* **Governance & Data Integrity**

  * **Medallion Architecture**: Data flows through structured Bronze → Silver → Gold layers, enabling clear lineage and traceability.
  * **Fail-Fast Orchestration**: Pipelines halt on failure to prevent partial loads and preserve data integrity.

* **Security & Config Management**

  * **Separation of Concerns**: Environment-specific configs (e.g., file paths, credentials) are isolated using `config.local.sql`, which is excluded from version control. This pattern ensures security without compromising reproducibility.

---

## 3. 🗂️ Project Structure

```
/
├── README.md                 → Project overview (this file)
├── .gitignore                → Ignores local-only or sensitive files
├── config.template.sql       → Template for local config setup
│
├── /data/                    → Raw CSVs (excluded from Git)
├── /docs/                    → Architecture diagrams, glossaries, etc.
│
└── /scripts/
    ├── 0_setup/              → DB creation, schema init
    ├── 1_bronze/             → Raw data layer scripts
    ├── 2_silver/             → Cleaned data layer scripts
    ├── 3_gold/               → Aggregated, business-ready data
    ├── 4_utils/              → Reusable helper functions & procs
    ├── 5_tests/              → Data quality checks & validations
    └── orchestration/        → Master pipeline execution scripts
```

---

## 4. 📚 Documentation

Each data layer (`bronze`, `silver`, `gold`) includes its own `README.md` 

---

## 5. ⚙️ How to Run This Project

### ✅ Environment Setup

1. Clone the repo.
2. Copy `config.template.sql` → `config.local.sql` and edit it with your local paths and secrets.

### 🏗️ Database & Schema Deployment

1. Run all scripts in `/scripts/0_setup/` to bootstrap the DB and base schemas.
2. Execute `config.local.sql` to populate dynamic config (e.g., file paths).
3. Deploy DDL scripts for Bronze, Silver, and Gold layers.

### 🧮 Stored Procedures

Run the procedure scripts (e.g., `proc_load_bronze.sql`) to define the loading logic in SQL Server.

### 🚀 Pipeline Execution

Run the orchestrator:

```sql
/scripts/orchestration/master_run_pipeline.sql
```

Monitor execution via the `dbo.etl_log` table to confirm success and debug issues if needed.

---

## 6. 📈 Current Status & Roadmap

* ✅ **Bronze Layer**: Complete and fully operational.
* 🔄 **Next Up**: Silver Layer in development—bringing structure, validation, and enrichment to raw data.

---

## 7. 🤝 Contact

This project demonstrates a deep understanding of scalable data warehousing architecture, ELT pipelines, and engineering best practices. If you're looking for someone to bring clarity and robustness to your data stack, let's connect.

**\[Kaoutar EL ouahabi]**
📧 \[[kaoutar.elouahabi.de@gmail.com](mailto:your.email@example.com)]
