# Bronze Layer

> Raw data enters here. The Bronze layer is the first stop in the medallion pipeline, where source CSV files are ingested, lightly profiled, and written to Delta tables with audit metadata.

## Purpose

The Bronze layer preserves the original shape of the real estate dataset while making it ready for downstream transformation. It is designed to:

- ingest each raw CSV from `Data/Raw`
- preserve source fidelity as much as possible
- attach lineage and ingestion metadata
- persist data as Delta tables for the Silver layer

This layer should stay simple, deterministic, and easy to rerun.

## What This Layer Does

The bronze ingestion logic in `ingestion.py` performs the following steps:

1. Reads the raw CSV file for a given table.
2. Prints schema information for validation.
3. Performs a null check across all columns.
4. Optionally checks duplicates using a primary key.
5. Adds audit columns:
   - `ingestion_time`
   - `source_file`
   - `layer`
6. Writes the result to a Delta table named `bronze_<table_name>`.
7. Verifies the final row count after write.

## Input Sources

The Bronze layer expects raw files in:

- `Data/Raw/agents.csv`
- `Data/Raw/customers.csv`
- `Data/Raw/properties.csv`
- `Data/Raw/listings.csv`
- `Data/Raw/interactions.csv`
- `Data/Raw/transactions.csv`
- `Data/Raw/customer_monthly_metrics.csv`

These files are treated as the system of record for ingestion.

## Output

Each source table is written to a Delta table using the pattern:

- `bronze_agents`
- `bronze_customers`
- `bronze_properties`
- `bronze_listings`
- `bronze_interactions`
- `bronze_transactions`
- `bronze_customer_monthly_metrics`

These tables are the handoff point for the Silver layer.

## Audit Columns

The Bronze layer adds minimal metadata to support traceability and debugging:

- `ingestion_time` captures when the record was loaded.
- `source_file` records the source CSV name.
- `layer` marks the record as bronze.

This makes it easier to track the origin of any record during later validation.

## Validation Philosophy

Bronze validation is intentionally lightweight:

- confirm the schema is readable
- inspect null distribution
- identify duplicate keys when a primary key is provided
- compare raw row count with Delta row count after load

The goal is not to clean data yet. The goal is to make sure the raw data landed safely and completely.

## Recommended Usage

Use the Bronze layer first whenever the raw files change or the pipeline is being rebuilt from scratch.

Example flow:

1. Load the raw CSV into Spark.
2. Run Bronze ingestion.
3. Review the Delta tables for completeness.
4. Pass the Bronze tables into Silver transformations.

## Design Notes

Keep the Bronze layer minimal:

- avoid business rules here
- avoid heavy transformations here
- do not drop important fields unless required for ingestion safety
- keep the code easy to rerun and easy to debug

If a rule looks like cleansing, standardization, or enrichment, it belongs in Silver, not Bronze.

## Files In This Folder

- `ingestion.py` - bronze ingestion logic for raw CSV files
- `README.md` - layer documentation and operational guidance

## Next Layer

After Bronze ingestion, the Silver layer cleans and reshapes the data for modeling, joins, and analytical use.
