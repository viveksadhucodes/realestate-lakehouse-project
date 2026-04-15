# 🏠 Real Estate Lakehouse Project

## 🚀 Project Overview

This project builds an **end-to-end Data Engineering pipeline** using:

* PySpark
* Delta Lake
* SQL
* Medallion Architecture (Bronze → Silver → Gold)

We process a **7-table Real Estate dataset** to generate insights like:

* Revenue trends
* Agent performance
* Property demand

---

# 📂 Project Structure (IMPORTANT)

```
realestate-lakehouse-project/
│
├── Data/
│   └── Raw/                     # All 7 CSV input files
│
├── Notebooks/                   # Execution layer (Databricks)
│   ├── 01_bronze.ipynb
│   ├── 02_silver.ipynb
│   └── 03_gold.ipynb
│
├── Scripts/
│   └── solution.py
│
├── docs/                        # Reference documents
│   ├── real_estate_dataset_summary.pdf
│   └── real_estate_project_plan.pdf
│
├── pipeline/                    # Core logic (clean, modular code)
│   ├── bronze/
│   │   └── ingestion.py
│   ├── gold/
│   │   └── analytics.py
│   ├── silver/
│   │   └── transformation.py
│   └── utils/
│       └── common.py
│
├── sql/                         # SQL queries for KPIs
│   └── kpi_queries.sql
│
└── README.md

```

---

# 🧠 Why This Structure Exists (Don’t Ignore This)

## 📁 Data/Raw/

* Stores original CSV files
* No modifications allowed
* Used for Bronze ingestion

👉 Purpose: Maintain **raw source of truth**

---

## 📁 Notebooks/

* Used to **run pipeline step-by-step** in Databricks
* Each notebook represents a layer:

  * Bronze → load data
  * Silver → clean + transform
  * Gold → analytics

👉 Purpose: Execution + demonstration

---

## 📁 Scripts/

* Additional Python scripts
* Utility or problem-solving code

👉 Purpose: Support scripts (not core pipeline)

---

## 📁 docs/

* Contains reference documents
* Project plan + dataset summary

👉 Purpose: Understanding before coding

---

## 📁 pipeline/

This is the **real project logic** (not notebooks).

### bronze/ingestion.py

* Reads CSVs
* Adds audit columns
* Writes Bronze Delta tables

### silver/transformation.py

* Cleans data (nulls, duplicates)
* Performs joins
* Implements MERGE
* Handles schema evolution

### gold/analytics.py

* Builds KPI tables
* Uses SQL + window functions
* Generates insights

### utils/common.py

* Shared helper functions

👉 Purpose: Clean, reusable, production-style code

---

## 📁 sql/

* Contains SQL queries for KPIs
* Used in Gold layer

👉 Purpose: Separate SQL logic from Python

---

# ⚠️ Important Notes

* Folder names must match exactly (case-sensitive in some systems)
* `Data/Raw` vs `data/raw` → don’t mix randomly
* Do NOT commit unnecessary files like `debug.log`

---

# 📌 Day 1 Instructions (MANDATORY)

## 1️⃣ Clone the Repository

```bash
git clone https://github.com/viveksadhucodes/realestate-lakehouse-project
cd realestate-lakehouse-project
```

---

## 2️⃣ Read Before Coding

Read:

* `docs/real_estate_project_plan.pdf`
* `docs/real_estate_dataset_summary.pdf`

These define:

* dataset structure
* join keys
* pipeline flow
* cleaning rules

If skipped:
👉 your joins WILL break later

---

## 3️⃣ Understand Core Concepts

### Dataset

* 7 tables
* Central fact table → **transactions**

### Keys

* customer_id
* property_id
* agent_id
* listing_id

### Pipeline Flow

```
CSV → Bronze → Silver → Gold
```

---

# 🧩 ER Diagram (SOURCE OF TRUTH)

```mermaid
erDiagram

    TRANSACTIONS {
        int transaction_id PK
        int customer_id FK
        int property_id FK
        int agent_id FK
        int listing_id FK
    }

    CUSTOMERS {
        int customer_id PK
    }

    PROPERTIES {
        int property_id PK
    }

    LISTINGS {
        int listing_id PK
        int property_id FK
        int agent_id FK
    }

    AGENTS {
        int agent_id PK
    }

    INTERACTIONS {
        int interaction_id PK
        int customer_id FK
        int property_id FK
    }

    CUSTOMER_MONTHLY_METRICS {
        int customer_id FK
    }

    TRANSACTIONS ||--o{ CUSTOMERS : "belongs to"
    TRANSACTIONS ||--o{ PROPERTIES : "involves"
    TRANSACTIONS ||--o{ AGENTS : "handled by"
    TRANSACTIONS ||--o{ LISTINGS : "linked to"

    LISTINGS ||--o{ PROPERTIES : "for"
    LISTINGS ||--o{ AGENTS : "managed by"

    INTERACTIONS ||--o{ CUSTOMERS : "by"
    INTERACTIONS ||--o{ PROPERTIES : "on"

    CUSTOMER_MONTHLY_METRICS ||--o{ CUSTOMERS : "aggregates"
```

---

# 👥 Team Responsibilities

| Member   | Role                    | Files                              |
| -------- | ----------------------- | ---------------------------------- |
| Member 1 | Bronze (Ingestion)      | pipeline/bronze, 01_bronze.ipynb   |
| Member 2 | Silver (Transformation) | pipeline/silver, 02_silver.ipynb   |
| Member 3 | Gold (Analytics)        | pipeline/gold, 03_gold.ipynb, sql/ |

---

# 🔄 Git Integration Workflow (USED IN THIS PROJECT)

Since Databricks Git integration is available, we follow a **direct Git workflow**.

---

## 🚀 Setup (One-Time)

In Databricks:

* New → **Git Folder**
* Paste repo URL
* Authenticate GitHub
* Clone repository

---

## 🔁 Daily Workflow

### 1️⃣ Pull Latest Code

```
git pull origin main
```

---

### 2️⃣ Work on Your Layer Only

* Do NOT modify other members’ files
* Stick to your assigned folder

---

### 3️⃣ Commit Changes

```
git add .
git commit -m "Silver: implemented cleaning and joins"
```

---

### 4️⃣ Push Changes

```
git push origin main
```

---

## ⚠️ Conflict Prevention

* Only ONE person edits a file at a time
* Always pull before starting work
* Push at end of session
* Use meaningful commit messages

---

# 🧠 Project Execution Plan (Aligned with 5-Day Plan)

* Day 1 → Setup + ER + roles
* Day 2 → Bronze layer
* Day 3 → Silver layer
* Day 4 → Gold KPIs + Delta features
* Day 5 → Full pipeline run + PPT

(Strictly follow plan) 

---

# ⚠️ Rules

* ❌ Do NOT assume column names
* ❌ Do NOT skip ER diagram
* ❌ Do NOT break pipeline for others
* ❌ Do NOT copy blindly

---

# 🧠 Goal of Day 1

* Dataset understanding
* ER diagram clarity
* Role assignment
* Databricks setup

---

# 🧨 Final Note

This project is not about writing code fast.

It is about:

* correct pipeline
* clean transformations
* clear explanation

If you don’t understand your own pipeline,
you will get exposed during the viva.

---
