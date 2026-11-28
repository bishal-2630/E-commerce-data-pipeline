# Olist E-commerce Analytics Pipeline

An end-to-end data engineering project: raw e-commerce CSV files are ingested to S3, loaded into Snowflake, transformed through a **Bronze → Silver → Gold** medallion architecture with dbt, orchestrated by Apache Airflow (with Astronomer Cosmos), and visualised in Apache Superset.

```
Olist CSVs → Airflow → AWS S3 → Snowflake BRONZE → SILVER → GOLD → Superset
                                          └──── dbt via Cosmos ────┘
                    (Apache Airflow orchestrates every step)
```

## Business use case

An online marketplace needs one trusted view of revenue, product performance and delivery quality. Its data arrives as nine raw CSV exports with inconsistent types, duplicates and missing values. This pipeline automates ingestion, cleaning and modelling, and answers:

1. How is revenue trending, and which categories drive it?
2. Where are deliveries slow or late?
3. Do late deliveries hurt customer reviews?

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle): 9 CSV files, about 100K orders, 2016 to 2018.

The CSVs are **not** stored in this repository. Download them and place all nine files in `include/data/raw/`.

| Table                             | Rows      |
| --------------------------------- | --------- |
| orders                            | 99,441    |
| order_items                       | 112,650   |
| order_payments                    | 103,886   |
| order_reviews                     | 99,224    |
| customers                         | 99,441    |
| products                          | 32,951    |
| sellers                           | 3,095     |
| geolocation                       | 1,000,163 |
| product_category_name_translation | 71        |

## Tech stack

| Layer             | Tool                       | Why                                                         |
| ----------------- | -------------------------- | ----------------------------------------------------------- |
| Orchestration     | Apache Airflow (Astro CLI) | Scheduling, retries, dependencies, monitoring               |
| Landing storage   | AWS S3                     | Cheap, durable, replayable raw files                        |
| Warehouse         | Snowflake                  | Separate storage and compute, native `COPY INTO` from S3    |
| Transformation    | dbt (dbt-snowflake)        | Version-controlled, testable, documented SQL                |
| dbt orchestration | Astronomer Cosmos          | Each dbt model becomes an Airflow task with visible lineage |
| Visualisation     | Apache Superset            | Open-source BI that reads the Gold layer                    |
| Environment       | Docker                     | Reproducible local setup                                    |

## Architecture

### Ingestion

- Airflow uploads each CSV to `s3://<bucket>/raw/<table>/ingest_date=YYYY-MM-DD/`.
- Snowflake `COPY INTO` loads each file into a Bronze table.
- **Idempotent:** a batch is deleted before it is reloaded, so reruns never duplicate data.
- Row counts are compared with the source file and written to `PIPELINE_RUN_LOG`. A mismatch fails the run.
- Retries (3, with exponential backoff) and an `on_failure_callback`.

### Medallion layers

|          | Bronze (`ECOM.BRONZE`)                                                              | Silver (`ECOM.SILVER`)       | Gold (`ECOM.GOLD`)                  |
| -------- | ----------------------------------------------------------------------------------- | ---------------------------- | ----------------------------------- |
| Purpose  | Raw data as received                                                                | Cleaned, deduplicated, typed | Business-ready models               |
| Built by | Airflow + `COPY INTO`                                                               | dbt via Cosmos               | dbt via Cosmos                      |
| Contents | 9 tables + run log, all `VARCHAR`, plus `_ingested_at`, `_source_file`, `_batch_id` | 9 `stg_*` models             | 4 dimensions, 2 facts, 4 aggregates |

**Gold models**

- Dimensions: `dim_customer`, `dim_product`, `dim_seller`, `dim_date`
- Facts: `fact_orders`, `fact_order_items`
- Aggregates: `agg_daily_sales`, `agg_monthly_revenue`, `agg_delivery_performance`, `agg_product_category_performance`

### Airflow DAGs

| DAG               | Purpose                                                                         |
| ----------------- | ------------------------------------------------------------------------------- |
| `master_pipeline` | Controller, `@daily`. Triggers the three DAGs below in order and waits for each |
| `ingestion_dag`   | CSV → S3 → Bronze, with validation and logging                                  |
| `dbt_silver`      | Cosmos `DbtDag` for `models/silver`                                             |
| `dbt_gold`        | Cosmos `DbtDag` for `models/gold`                                               |

`master_pipeline`: `start → failure_demo → run_ingestion → run_dbt_silver → run_dbt_gold → notify_success`

If any stage fails, all downstream tasks are marked `upstream_failed`, so broken data never reaches Gold. To demo this, trigger `master_pipeline` with `simulate_failure = true`.

### Security

- `AIRFLOW_ROLE` runs the pipeline. `SUPERSET_READER` can only read `ECOM.GOLD`.
- Credentials live in Airflow connections and Git-ignored `.env` files, never in code.

## Project structure

```
.
├── dags/
│   ├── ingestion_dag.py
│   ├── dbt_silver_dag.py
│   ├── dbt_gold_dag.py
│   └── master_pipeline_dag.py
├── include/
│   ├── data/raw/              # Olist CSVs (not in Git)
│   └── dbt_project/
│       ├── dbt_project.yml
│       ├── macros/generate_schema_name.sql
│       └── models/
│           ├── silver/        # stg_* models and sources.yml
│           └── gold/          # dim_*, fact_*, agg_* models
├── superset/                  # Dockerfile, docker-compose.yml, config
├── Dockerfile                 # Astro Runtime + dbt virtualenv
├── requirements.txt
└── README.md
```

## How to run

### Prerequisites

- Docker Desktop (at least 6 GB RAM allocated)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- An AWS account with an S3 bucket
- A Snowflake account

### 1. Clone and add the data

```bash
git clone https://github.com/bishal-2630/E-commerce-data-pipeline.git
cd E-commerce-data-pipeline
```

Place the nine Olist CSVs in `include/data/raw/`.

### 2. AWS

- Create an S3 bucket and set its name in `BUCKET` in `dags/ingestion_dag.py`.
- Create an IAM user for Airflow with `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` and `s3:ListBucket` on the bucket.
- Create a second, read-only IAM user for Snowflake (`s3:GetObject`, `s3:ListBucket`).

### 3. Snowflake

Run once as `ACCOUNTADMIN`:

```sql
CREATE DATABASE IF NOT EXISTS ECOM;
CREATE SCHEMA IF NOT EXISTS ECOM.BRONZE;
CREATE SCHEMA IF NOT EXISTS ECOM.SILVER;
CREATE SCHEMA IF NOT EXISTS ECOM.GOLD;

CREATE FILE FORMAT IF NOT EXISTS ECOM.BRONZE.CSV_FF
  TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL') EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE IF NOT EXISTS ECOM.BRONZE.RAW_STAGE
  URL = 's3://<your-bucket>/raw/'
  CREDENTIALS = (AWS_KEY_ID = '<read-only key>' AWS_SECRET_KEY = '<read-only secret>')
  FILE_FORMAT = ECOM.BRONZE.CSV_FF;

CREATE ROLE IF NOT EXISTS AIRFLOW_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AIRFLOW_ROLE;
GRANT ALL ON DATABASE ECOM TO ROLE AIRFLOW_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE ECOM TO ROLE AIRFLOW_ROLE;
GRANT USAGE ON FILE FORMAT ECOM.BRONZE.CSV_FF TO ROLE AIRFLOW_ROLE;
GRANT USAGE ON STAGE ECOM.BRONZE.RAW_STAGE TO ROLE AIRFLOW_ROLE;
GRANT ROLE AIRFLOW_ROLE TO ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS AIRFLOW_USER PASSWORD = '<password>'
  TYPE = LEGACY_SERVICE DEFAULT_ROLE = AIRFLOW_ROLE DEFAULT_WAREHOUSE = COMPUTE_WH;
GRANT ROLE AIRFLOW_ROLE TO USER AIRFLOW_USER;
```

### 4. Start Airflow

```bash
astro dev start
```

Open http://localhost:8080 (check `astro dev ps` if the port differs).

Create two connections under **Admin → Connections**:

| Connection Id       | Type                | Key fields                                                                                                                                                            |
| ------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `aws_default`       | Amazon Web Services | Airflow IAM user's access key and secret, region in Extra                                                                                                             |
| `snowflake_default` | Snowflake           | Login `AIRFLOW_USER`, Account `<orgname>-<accountname>` (no `.snowflakecomputing.com`), Warehouse `COMPUTE_WH`, Database `ECOM`, Schema `BRONZE`, Role `AIRFLOW_ROLE` |

### 5. Run the pipeline

Trigger **`master_pipeline`** in the Airflow UI. A full run takes several minutes and builds Bronze, Silver and Gold.

### 6. Superset dashboards

Create a read-only Snowflake user:

```sql
CREATE ROLE IF NOT EXISTS SUPERSET_READER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SUPERSET_READER;
GRANT USAGE ON DATABASE ECOM TO ROLE SUPERSET_READER;
GRANT USAGE ON SCHEMA ECOM.GOLD TO ROLE SUPERSET_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA ECOM.GOLD TO ROLE SUPERSET_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOM.GOLD TO ROLE SUPERSET_READER;
CREATE USER IF NOT EXISTS SUPERSET_USER PASSWORD = '<password>'
  TYPE = LEGACY_SERVICE DEFAULT_ROLE = SUPERSET_READER DEFAULT_WAREHOUSE = COMPUTE_WH;
GRANT ROLE SUPERSET_READER TO USER SUPERSET_USER;
```

Then, in `superset/`, create a `.env` file containing `SUPERSET_SECRET_KEY=<long random string>` and run:

```bash
docker compose up -d --build
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password <password>
docker exec -it superset superset init
```

Open http://localhost:8088, add a Snowflake database with this SQLAlchemy URI, and register the `GOLD` tables as datasets:

```
snowflake://SUPERSET_USER:<password>@<orgname>-<accountname>/ECOM/GOLD?warehouse=COMPUTE_WH&role=SUPERSET_READER
```

## Dashboards

**Executive Overview:** total revenue, total orders, average order value, monthly revenue trend, top 10 categories.
**Operations:** on-time delivery % by state, average delivery days by state, review score for late vs on-time orders.

Key findings:

- Revenue grew strongly through 2017, peaking around November 2017.
- Late deliveries are associated with a drop in average review score from about 4.3 to about 2.6.
- Delivery time varies about 3x across Brazil. Alagoas has the lowest on-time rate.

## Author

Bishal Khadka
