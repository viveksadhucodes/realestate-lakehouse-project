# Real Estate Lakehouse Project

<div align="center">

### End-to-End Medallion Data Pipeline (Bronze -> Silver -> Gold)

[![Domain](https://img.shields.io/badge/Domain-Data%20Engineering-0A66C2?style=for-the-badge)](README.md)
[![Architecture](https://img.shields.io/badge/Pattern-Medallion-0E9F6E?style=for-the-badge)](README.md)
[![Engine](https://img.shields.io/badge/Engine-PySpark-F97316?style=for-the-badge)](README.md)
[![Storage](https://img.shields.io/badge/Storage-Delta%20Lake-7C3AED?style=for-the-badge)](README.md)

</div>

---

## Project Status

Project complete. The repository contains:

- Full Bronze ingestion pipeline
- Full Silver transformation and validation pipeline
- Full Gold analytics/KPI pipeline
- Executable notebooks for all three layers
- SQL KPI query file
- Layer-wise documentation

### Delivery Snapshot

| Dimension | Status | Notes |
|---|---|---|
| Data Pipeline | Complete | Bronze -> Silver -> Gold implemented |
| Data Quality | Complete | Validation checks embedded in notebooks |
| Analytics | Complete | KPI suite in Gold layer |
| Documentation | Complete | Root + layer-level READMEs |
| Reproducibility | Complete | Ordered notebook execution path |

---

## Quick Navigation

- [Project Overview](#project-overview)
- [Final Repository Structure](#final-repository-structure)
- [Data Sources](#data-sources)
- [Architecture](#architecture)
- [Pipeline Implementation](#pipeline-implementation)
- [Notebook Flow](#notebook-flow)
- [KPI Outputs](#kpi-outputs)
- [How To Run](#how-to-run)
- [Tech Stack](#tech-stack)
- [Documentation Index](#documentation-index)
- [Team Responsibilities](#team-responsibilities)
- [References](#references)

---

## Project Overview

This project builds a production-style lakehouse workflow for a 7-table real estate dataset.

Data moves through the medallion model:

- Bronze: raw ingestion and audit metadata
- Silver: cleaning, standardization, deduplication, and fact build
- Gold: KPI modeling and analytics outputs

Business questions addressed include:

- Revenue trends across cities, categories, and time
- Agent performance and ranking
- Listing conversion and market speed
- Customer purchase behavior and segmentation

### Why This Project Is Production-Oriented

- Clear separation of concerns across Medallion layers
- Reusable Python modules under pipeline/ for notebook orchestration
- Explicit quality checks before analytical consumption
- Structured outputs suitable for BI/dashboard integration

---

## Final Repository Structure

```text
realestate-lakehouse-project/
|
|-- Data/
|   |-- Raw/
|   |   |-- agents.csv
|   |   |-- customer_monthly_metrics.csv
|   |   |-- customers.csv
|   |   |-- interactions.csv
|   |   |-- listings.csv
|   |   |-- properties.csv
|   |   |-- transactions.csv
|
|-- docs/
|   |-- real_estate_dataset_summary.pdf
|   |-- real_estate_project_plan.pdf
|
|-- Notebooks/
|   |-- 01_bronze.ipynb
|   |-- 02_silver.ipynb
|   |-- 03_gold.ipynb
|
|-- pipeline/
|   |-- bronze/
|   |   |-- ingestion.py
|   |   |-- README.md
|   |-- silver/
|   |   |-- transformation.py
|   |   |-- README.md
|   |-- gold/
|   |   |-- analytics.py
|   |   |-- README.md
|
|-- sql/
|   |-- kpi_queries.sql
|
|-- README.md
```

---

## Data Sources

Raw input tables used:

- agents
- customers
- properties
- listings
- interactions
- transactions
- customer_monthly_metrics

Raw files are treated as source-of-truth and are stored in Data/Raw.

---

## Architecture

```mermaid
graph LR
    A[Raw CSV Files] --> B[Bronze Layer]
    B --> C[Silver Layer]
    C --> D[Gold Layer]

    B --> B1[Audit Columns]
    C --> C1[Cleaned Dimensions]
    C --> C2[Integrated Fact]
    D --> D1[KPI Tables and Views]
```

### Layer Contracts

| Layer | Input | Core Processing | Output |
|---|---|---|---|
| Bronze | Raw CSV files | Ingestion + audit metadata + basic profiling | bronze_* Delta tables |
| Silver | Bronze tables | Cleansing, standardization, deduplication, joins | silver_* dimensions + fact |
| Gold | Silver fact | KPI aggregation, ranking, trend analytics | KPI-ready analytical datasets |

### Entity Relationship Snapshot

```mermaid
erDiagram
    TRANSACTIONS {
        string transaction_id PK
        string customer_id FK
        string property_id FK
        string agent_id FK
    }

    CUSTOMERS {
        string customer_id PK
    }

    PROPERTIES {
        string property_id PK
    }

    LISTINGS {
        string listing_id PK
        string property_id FK
        string agent_id FK
    }

    AGENTS {
        string agent_id PK
    }

    INTERACTIONS {
        string interaction_id PK
        string customer_id FK
        string property_id FK
    }

    CUSTOMER_MONTHLY_METRICS {
        string customer_id FK
    }

    TRANSACTIONS ||--o{ CUSTOMERS : belongs_to
    TRANSACTIONS ||--o{ PROPERTIES : involves
    TRANSACTIONS ||--o{ AGENTS : handled_by

    PROPERTIES ||--o{ LISTINGS : listed_as
    LISTINGS ||--o{ AGENTS : managed_by

    CUSTOMERS ||--o{ INTERACTIONS : performs
    PROPERTIES ||--o{ INTERACTIONS : receives

    CUSTOMERS ||--o{ CUSTOMER_MONTHLY_METRICS : aggregates
```

---

## Pipeline Implementation

### Technical Highlights

- Modular engineering: transformation logic is isolated from notebook orchestration
- Deterministic deduplication strategy for stable transaction grain
- Window functions for ranking and cumulative analytics
- Business-rule enforcement to protect metric correctness
- Schema-aware overwrite workflow for iterative development

### Bronze Layer

Implemented in pipeline/bronze/ingestion.py.

Key features:

- CSV ingestion for each source table
- Schema print and null analysis
- Optional duplicate check by primary key
- Audit columns:
  - ingestion_time
  - source_file
  - layer
- Delta write as bronze_<table_name>
- Row-count validation after write

Layer documentation: pipeline/bronze/README.md

### Silver Layer

Implemented in pipeline/silver/transformation.py and orchestrated in Notebooks/02_silver.ipynb.

Key features:

- Entity-level cleaning functions:
  - clean_transactions
  - clean_customers
  - clean_properties
  - clean_agents
  - clean_listings
- Standardization: trim/lowercase and category normalization
- Derived features: year_month, price_category, property_age, experience_level
- Controlled deduplication to prevent row explosion
- Integrated fact build via build_silver_fact
- Validation suite:
  - schema checks
  - null checks
  - duplicate checks
  - business-rule checks
  - join-integrity checks

Silver outputs:

- silver.silver_customers
- silver.silver_properties
- silver.silver_agents
- silver.silver_listings
- silver.silver_real_estate_fact

Layer documentation: pipeline/silver/README.md

### Gold Layer

Implemented in pipeline/gold/analytics.py and orchestrated in Notebooks/03_gold.ipynb.

Key features:

- KPI-focused transformations on Silver fact
- Window functions for ranking and cumulative metrics
- Demand, performance, conversion, and behavioral analytics

Sample KPI functions:

- revenue_by_city
- monthly_sales
- top_agents
- property_demand
- listing_conversion_rate
- commission_efficiency
- market_speed_analysis
- fastest_selling_category

Layer documentation: pipeline/gold/README.md

---

## Data Quality Gates

Quality controls applied during pipeline execution:

| Check Type | Layer | Example |
|---|---|---|
| Null validation | Bronze/Silver | Critical key and field-level null checks |
| Duplicate validation | Bronze/Silver | transaction_id uniqueness verification |
| Business-rule validation | Silver | commission_amount <= deal_price |
| Join-integrity validation | Silver | Missing dimension coverage analysis |
| Temporal sanity checks | Silver | Future-date detection for deal_date |

These checks reduce silent data corruption before analytics are produced.

---

## Notebook Flow

Recommended execution sequence:

1. Run Notebooks/01_bronze.ipynb
2. Run Notebooks/02_silver.ipynb
3. Run Notebooks/03_gold.ipynb

This ensures all upstream tables are available before downstream logic runs.

---

## KPI Outputs

Gold layer KPIs are designed for dashboard consumption and stakeholder reporting.

Main KPI groups:

- Revenue and sales trend KPIs
- Agent performance and ranking KPIs
- Listing conversion and market speed KPIs
- Buyer behavior and premium segment KPIs

### KPI Matrix

| KPI Family | Example Functions | Business Value |
|---|---|---|
| Revenue Intelligence | revenue_by_city, monthly_sales, running_revenue | Understand growth and trend direction |
| Agent Performance | top_agents, agent_ranking, commission_efficiency | Measure sales execution and contribution |
| Market Dynamics | property_demand, market_speed_analysis, fastest_selling_category | Track demand and liquidity behavior |
| Customer Behavior | customer_purchase_frequency, buyer_type, high_value_buyers | Segment and target customer profiles |
| Channel Effectiveness | listing_conversion_rate | Optimize listing channel strategy |

Additional SQL-based KPI logic can be maintained in sql/kpi_queries.sql.

---

## How To Run

### 1) Clone Repository

```bash
git clone https://github.com/viveksadhucodes/realestate-lakehouse-project
cd realestate-lakehouse-project
```

### 2) Load Environment

Use Databricks (or compatible Spark environment) with Delta support.

### 3) Execute Notebooks in Order

- Notebooks/01_bronze.ipynb
- Notebooks/02_silver.ipynb
- Notebooks/03_gold.ipynb

### 4) Verify Tables

Check Bronze, Silver, and Gold schema outputs and validation sections in the notebooks.

### 5) Optional SQL Analysis

Use sql/kpi_queries.sql for additional KPI slicing and reporting workflows.

---

## Tech Stack

- PySpark
- Delta Lake
- SQL
- Databricks Notebooks
- Git/GitHub

---

## Documentation Index

- Root overview: README.md
- Bronze details: pipeline/bronze/README.md
- Silver details: pipeline/silver/README.md
- Gold details: pipeline/gold/README.md
- SQL KPIs: sql/kpi_queries.sql

---

## Team Responsibilities

| Member | Responsibility | Primary Assets |
|---|---|---|
| Member 1 | Bronze ingestion | pipeline/bronze, Notebooks/01_bronze.ipynb |
| Member 2 | Silver transformation | pipeline/silver, Notebooks/02_silver.ipynb |
| Member 3 | Gold analytics | pipeline/gold, Notebooks/03_gold.ipynb, sql/ |

---

## References

- docs/real_estate_project_plan.pdf
- docs/real_estate_dataset_summary.pdf

---

## Final Note

This project demonstrates the full lifecycle of a lakehouse pipeline:

- data ingestion reliability
- transformation quality and governance
- business-ready analytics

The repository is now organized as a complete, handover-ready implementation.

### Next Technical Enhancements

- Add automated data tests for critical table contracts
- Add orchestration (scheduled jobs/workflows) for periodic refresh
- Add KPI versioning and benchmark tracking
- Add CI checks for notebook and module consistency
