# 🏠 Real Estate Lakehouse Project

## 🚀 Project Overview

This project aims to build an **end-to-end Data Engineering pipeline** using:

- PySpark
- Delta Lake
- SQL
- Medallion Architecture (Bronze → Silver → Gold)

We will process a **Real Estate dataset (7 tables)** and generate business insights such as revenue trends, agent performance, and property demand.

---

# 📂 Repository Structure

```
realestate-lakehouse-project/
│
├── Data/              # Raw CSV files
├── Notebooks/         # Bronze, Silver, Gold notebooks
├── Scripts/           # Python scripts (Delta operations etc.)
├── Screenshots/       # Output screenshots
├── README.md
├── real_estate_project_plan.pdf
└── real_estate_dataset_summary.pdf
```

---

# 📌 What You Should Do FIRST (Day 1 Instructions)

Before touching any code, do the following:

## 1️⃣ Clone the Repository

```bash
git clone <your-repo-link>
cd realestate-lakehouse-project
```

---

## 2️⃣ Understand the Project (MANDATORY)

You are NOT allowed to start coding yet.

Read these two files carefully:

- 📄 `real_estate_project_plan.pdf` → Execution plan (Day 1–5)
- 📄 `real_estate_dataset_summary.pdf` → Dataset + schema details

---

## 3️⃣ What You Should Understand After Reading

If you don’t understand these, you’re already behind:

### 🔹 Dataset Understanding

- What are the 7 tables?
- Which is the **main fact table**? (transactions)
- What are the join keys?
  - customer_id
  - property_id
  - agent_id
  - listing_id

---

### 🔹 Architecture Understanding

- What is Bronze, Silver, Gold?
- Why Bronze has no transformations
- Why Silver handles cleaning + joins
- Why Gold contains KPIs

---

### 🔹 Pipeline Flow

- How data moves:

  ```
  CSV → Bronze → Silver → Gold
  ```

---

## 4️⃣ Team Discussion (15–20 mins)

All members must sit together and:

- Confirm dataset is correct
- Discuss relationships between tables
- Draw ER diagram (on paper or tool)
- Agree on naming conventions (snake_case)

---

## 5️⃣ Role Assignment (STRICT)

Each member must own ONE layer:

### 👤 Member 1 – Ingestion Engineer

- Handles Bronze layer
- Loads all CSV files
- Adds audit columns

---

### 👤 Member 2 – Transformation Engineer

- Handles Silver layer
- Cleaning + joins
- MERGE + schema evolution

---

### 👤 Member 3 – Analytics Engineer

- Handles Gold layer
- KPIs + SQL + window functions
- Documentation + PPT

---

## 6️⃣ Setup Databricks Environment

- Create workspace
- Upload CSV files to DBFS:

  ```
  /FileStore/project/raw/
  ```

- Create folder structure:

  ```
  /raw/
  /bronze/
  /silver/
  /gold/
  ```

---

# ⚠️ Important Rules

- ❌ Do NOT start coding before understanding dataset
- ❌ Do NOT assume column names (use actual CSV columns like deal_price, deal_date)
- ❌ Do NOT skip ER diagram
- ❌ Do NOT copy blindly from tutorials

---

# 🧠 Goal of Day 1

By the end of Day 1, you should have:

- Clear understanding of dataset
- ER diagram ready
- Roles assigned
- Databricks setup complete

If this is not done properly:
👉 The rest of the project will become difficult

---

# 🧨 Final Note

This project is NOT about writing code fast.
It is about building a **correct, explainable pipeline**.

If you don’t understand what you are doing,
you will get exposed during the viva.

---
