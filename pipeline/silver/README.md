# Silver Layer

The Silver layer is where raw Bronze data becomes analytics-ready, reliable, and consistent.

It applies quality rules, standardization, deduplication, and entity integration to produce clean dimension tables plus a unified fact table.

## Why Silver Exists

Bronze preserves source data as-is. Silver makes that data usable.

This layer is responsible for:

- enforcing core data quality rules
- standardizing text and categorical values
- deduplicating records on business keys
- deriving analytical columns
- integrating entities into a transaction-level fact table
- validating output quality before Gold consumption

## Source of Logic

Silver behavior is defined by these two artifacts:

- `pipeline/silver/transformation.py`
- `Notebooks/02_silver.ipynb`

The Python module contains reusable transformation functions.
The notebook orchestrates loading, cleaning, writing, and validating Silver outputs.

## End-to-End Silver Flow

1. Set catalog/schema context.
2. Read Bronze tables.
3. Apply entity-level cleaning functions.
4. Save cleaned dimension tables in Silver schema.
5. Build integrated fact table with controlled joins.
6. Write final fact table.
7. Run validation suite and summary diagnostics.

## Input Tables (Bronze)

The notebook reads these Bronze tables:

- `bronze.bronze_transactions`
- `bronze.bronze_customers`
- `bronze.bronze_properties`
- `bronze.bronze_agents`
- `bronze.bronze_listings`

## Output Tables (Silver)

The Silver pipeline writes:

- `silver.silver_customers`
- `silver.silver_properties`
- `silver.silver_agents`
- `silver.silver_listings`
- `silver.silver_real_estate_fact`

All writes are done in overwrite mode with schema overwrite enabled where applicable.

## Transformation Rules by Entity

### 1) Transactions (`clean_transactions`)

Quality filters:

- `deal_price > 0`
- `commission_amount > 0`
- `commission_amount < deal_price`
- `deal_date` not null
- `transaction_id` not null
- deduplicate by `transaction_id`

Standardization:

- fill null `payment_mode` as `unknown`
- normalize `payment_mode`, `city`, `deal_status` using trim + lowercase

Normalization:

- map `complete` and `completed` to `completed`
- map `canceled` and `cancelled` to `cancelled`
- all other statuses to `unknown`

Derived columns:

- `year_month` (`yyyy-MM` from `deal_date`)
- `deal_year`
- `price_category` (`low`, `mid`, `high`, `luxury`)

### 2) Customers (`clean_customers`)

Quality filters:

- `customer_id` not null
- `age > 0`
- deduplicate by `customer_id`

Standardization:

- fill nulls in `segment`, `home_city`, `income_band`, `acquisition_channel` as `unknown`
- lowercase and trim relevant text columns

Derived columns:

- `signup_year` from `signup_date`

### 3) Properties (`clean_properties`)

Quality filters:

- `property_id` not null
- `size_sqm > 0`
- `list_price > 0`
- deduplicate by `property_id`

Standardization:

- fill null `property_type` and `city` as `unknown`
- lowercase + trim both columns

Derived columns:

- `price_per_sqm = list_price / size_sqm`
- `property_age = year(current_date()) - year_built`
- `property_category` (`low`, `mid`, `high`, `luxury`)

### 4) Agents (`clean_agents`)

Quality filters:

- `agent_id` not null
- `agent_rating` between 0 and 5
- deduplicate by `agent_id`

Standardization:

- fill null `agent_city` as `unknown`
- lowercase + trim `agent_city`

Derived columns:

- `experience_level` (`junior`, `mid`, `senior`)
- `commission_category` (`low`, `medium`, `high`)

### 5) Listings (`clean_listings`)

Quality filters:

- `listing_id` not null
- `property_id` not null
- `listed_price > 0`
- `days_on_market >= 0`

Deduplication strategy:

- window by `property_id`
- keep top row by highest `listed_price`
- this prevents row explosion during fact joins

Standardization:

- fill null `listing_channel` as `unknown`
- lowercase + trim `listing_channel`

## Fact Assembly (`build_silver_fact`)

The fact pipeline creates a transaction-grain integrated table.

Pre-join protections:

- deduplicate all source inputs on business keys
- enforce one-row-per-property in listings before join

Selected columns are projected from each cleaned entity, then joined:

- transaction base
- left join customers on `customer_id`
- left join properties on `property_id`
- left join agents on `agent_id`
- left join listings on `property_id`

Post-join controls:

- `city_variation_flag = 1` when `city` and `property_city` mismatch or are null
- final deduplication by `transaction_id`

This protects the table against duplicate transactions and unstable join behavior.

## Silver Validation Suite (Notebook)

After write, the notebook runs a detailed validation report:

- schema print for the fact table
- sample preview
- total record and column count
- future date check on `deal_date`
- city consistency distribution via `city_variation_flag`
- full-column null scan
- duplicate check on `transaction_id`
- commission business rule check (`commission_amount <= deal_price`)
- join integrity metrics (missing customer/property/agent enrichment)
- catalog table listing in Silver schema

Final diagnostics also include:

- retention rate from Bronze transactions to Silver fact
- dropped-row analysis using left anti join
- critical key null checks
- listing join completeness check
- production readiness status output

## Data Contract for Gold Layer

The Silver fact table provides:

- stable transaction keys and entity foreign keys
- cleaned dimensions (customer/property/agent/listing context)
- validated measures (`deal_price`, `commission_amount`)
- derived temporal and segmentation fields (`year_month`, categories)
- quality indicator (`city_variation_flag`)

Gold layer should consume this table instead of direct Bronze tables.

## Execution Notes

Typical execution order:

1. Run Bronze ingestion first.
2. Run `Notebooks/02_silver.ipynb` end-to-end.
3. Confirm validation output is healthy.
4. Proceed to Gold transformations.

## Troubleshooting

If record counts drop unexpectedly:

- inspect transaction quality filters in `clean_transactions`
- run dropped-row analysis block from the notebook summary section

If join completion is low:

- validate key integrity in Bronze tables (`customer_id`, `property_id`, `agent_id`)
- inspect deduplication assumptions in listings by `property_id`

If duplicate transactions appear:

- verify upstream Bronze duplicates
- confirm final `dropDuplicates(["transaction_id"])` executed before write

## Folder Contents

- `transformation.py`: reusable Silver transformation functions
- `README.md`: architecture, business rules, and validation guidance for Silver
