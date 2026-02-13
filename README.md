# Azure Medallion Lakehouse Framework
### *Enterprise Data Architecture with Databricks, Delta Lake, and Unity Catalog*

## 📌 Project Vision
This repository serves as a production-grade blueprint for implementing a **Medallion Architecture** (Bronze, Silver, Gold). The framework focuses on **modularization**, **reusability**, and **performance optimization**, ensuring a scalable data estate that remains cost-efficient.

## 🏗️ Folder Structure
- `notebooks/`: Modular ETL logic for the Bronze, Silver, and Gold layers.
- `config/`: JSON/YAML configuration files to drive environment-based parameters.
- `utils/`: Common library of reusable PySpark functions (Salting, Logging, Validation).
- `cicd/`: Azure DevOps YAML pipelines for automated code promotion.

## 🚀 Architectural Layers
1. **Bronze (Raw):** Low-latency ingestion from ADLS Gen2 using **Autoloader**.
2. **Silver (Enriched):** Data cleansing, schema enforcement, and **SCD Type 2** logic.
3. **Gold (Business):** Aggregate modeling with **Liquid Clustering** for high-speed Power BI consumption.

## 🛠️ Design Principles
- **Idempotency:** Pipelines can be re-run without duplicate data creation.
- **Data Quality:** Integrated **Expectations** to enforce data integrity.
- **Governance:** Engineered for **Unity Catalog** with fine-grained access control.
- **Cost Efficiency:** Strategic use of **Adaptive Query Execution (AQE)** and **Z-Ordering**.

## 📫 Contact
For architectural discussions or implementation queries, connect via (https://ae.linkedin.com/in/nivinceapen)
