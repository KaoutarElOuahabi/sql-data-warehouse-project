# 📦 Enterprise SQL Data Warehouse: A Blueprint for Scalable Analytics

## 🎯 Project Goal

This repository provides a production-grade SQL Server data warehouse designed to be more than just a data dump — it's an engineered system built to be the *single source of truth* for enterprise analytics.

The primary objective is to offer a reference architecture that demonstrates **best practices** in enterprise data warehousing: from ingestion and transformation to orchestration, governance, and operational integrity. Built for scale and designed for clarity, this project can serve both as a blueprint and as a starting point for real-world deployments.

---

## 🧠 Architectural Philosophy & Best Practices

This project is shaped by modern data engineering principles to support long-term agility, robustness, and team collaboration.

### 🔑 Core Pillars

- **Scalability & Performance**  
  The system is built to handle growing data volumes and complex analytical queries with ease. The architecture prioritizes reliability in the early layers and speeds up access in the refined layers through proper indexing, data typing, and aggregation strategies.

- **Maintainability & Collaboration**  
  A clean directory structure, modular SQL, and separated configuration files ensure the codebase is easy to maintain and built for teamwork.

- **Governance & Data Integrity**  
  - **Medallion Architecture**: Data flows through structured Bronze → Silver → Gold layers, enabling clear lineage and traceability.  
  - **Fail-Fast Orchestration**: Pipelines halt on failure to prevent partial loads and preserve data integrity.

- **Security & Config Management**  
  - **Separation of Concerns**: Environment-specific configs (e.g., file paths, credentials) are isolated using `config.local.sql`, which is excluded from version control. This pattern ensures security without compromising reproducibility.

---

## 🗂️ Project Structure

```

/
├── README.md                 → Project overview (this file)
├── .gitignore                → Ignores local-only or sensitive files
├── config.template.sql       → Template for local config setup
│
├── /data/                    → Raw CSVs (excluded from Git)
├── /docs/                    → Architecture diagrams, glossaries, etc.
│
├── /scripts/
│   ├── 0\_setup/              → DB creation, schema init
│   ├── 1\_bronze/             → Raw data layer scripts
│   ├── 2\_silver/             → Cleaned data layer scripts
│   └── 3\_gold/               → Aggregated, business-ready data
│
├── /utils/                   → Reusable helper functions & procs
├── /tests/                   → Data quality checks & validations
└── /orchestration/           → Master pipeline execution scripts



```

---

## 📚 Documentation

Each data layer (`bronze`, `silver`, `gold`) includes its own `README.md`.

---

## ⚙️ How to Run This Project

### ✅ Environment Setup

1. Clone the repository.
2. Copy `config.template.sql` → `config.local.sql` and edit it with your local paths and secrets.

### 🏗️ Database Deployment

1. Execute the scripts in `/scripts/0_setup/` to create the database, schemas, and helper tables.
2. Run your `config.local.sql` script to populate the file list configuration.
3. Execute the DDL scripts for each layer (e.g., `ddl_bronze.sql`, `ddl_silver.sql`).

### 🛠️ Procedure Deployment

Execute the procedure scripts for each layer (e.g., `proc_load_bronze.sql`) to create the stored procedures in the database.

### 🚀 Pipeline Execution

1. Run the master script located at `/scripts/orchestration/master_run_pipeline.sql` to execute the end-to-end ELT process.
2. Monitor the output and check the `dbo.etl_log` table to verify the results of the run.

---

## 📊 Current Status & Next Steps

- **Status**: Bronze & Silver Layers Complete. The data ingestion, transformation, and quality testing frameworks are fully functional.
- **Next Step (En cours...)**: Build the Gold Layer

---

## 🤝 Contact

This project demonstrates a deep understanding of scalable data warehousing architecture, ELT pipelines, and engineering best practices. If you're looking for someone to bring clarity and robustness to your data stack, let's connect.

**Kaoutar EL Ouahabi**  
📧 [kaoutar.elouahabi.de@gmail.com](mailto:kaoutar.elouahabi.de@gmail.com)

