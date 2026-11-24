import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, get_current_context

RAW_DIR = Path("/usr/local/airflow/include/data/raw")
BUCKET = "bishalstorage"
DB, SCHEMA, STAGE = "ECOM", "BRONZE", "@ECOM.BRONZE.RAW_STAGE"

# table name in Bronze -> source file
TABLES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

log = logging.getLogger(__name__)


def alert_on_failure(context):
    ti = context["task_instance"]
    log.error("PIPELINE FAILURE: dag=%s task=%s map_index=%s",
              ti.dag_id, ti.task_id, ti.map_index)
    


def run_date() -> str:
    ctx = get_current_context()
    return ctx.get("ds") or datetime.utcnow().strftime("%Y-%m-%d")


def read_header(path: Path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f))


def count_rows(path: Path) -> int:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.reader(f)) - 1 


@dag(
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=3,  
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": alert_on_failure,
    },
    tags=["ingestion", "bronze"],
)
def ingestion_dag():

    @task
    def init_log_table():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        SnowflakeHook(snowflake_conn_id="snowflake_default").run(f"""
            CREATE TABLE IF NOT EXISTS {DB}.{SCHEMA}.PIPELINE_RUN_LOG (
                BATCH_ID VARCHAR, TABLE_NAME VARCHAR,
                SOURCE_ROWS NUMBER, LOADED_ROWS NUMBER,
                STATUS VARCHAR, LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )""")

    @task
    def upload_to_s3(table: str) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        path = RAW_DIR / TABLES[table]
        if not path.exists():
            raise FileNotFoundError(f"Missing source file: {path}")
        batch = run_date()
        key = f"raw/{table}/ingest_date={batch}/{table}.csv"
        S3Hook(aws_conn_id="aws_default").load_file(
            filename=str(path), key=key, bucket_name=BUCKET, replace=True
        )
        rows = count_rows(path)
        log.info("Uploaded %s (%s rows) to s3://%s/%s", table, rows, BUCKET, key)
        return {"table": table, "batch": batch, "source_rows": rows}

    @task
    def load_bronze(info: dict) -> dict:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        table, batch = info["table"], info["batch"]
        full = f"{DB}.{SCHEMA}.{table.upper()}"
        header = read_header(RAW_DIR / TABLES[table])

        cols_ddl = ", ".join(f'"{c.upper()}" VARCHAR' for c in header)
        cols = ", ".join(f'"{c.upper()}"' for c in header)
        selects = ", ".join(f"${i + 1}" for i in range(len(header)))
        path = f"{table}/ingest_date={batch}/"

        hook.run([
            f"""CREATE TABLE IF NOT EXISTS {full} ({cols_ddl},
                _INGESTED_AT TIMESTAMP_NTZ, _SOURCE_FILE VARCHAR, _BATCH_ID VARCHAR)""",
            
            f"DELETE FROM {full} WHERE _BATCH_ID = '{batch}'",
            
            f"""COPY INTO {full} ({cols}, _INGESTED_AT, _SOURCE_FILE, _BATCH_ID)
                FROM (SELECT {selects}, CURRENT_TIMESTAMP(), METADATA$FILENAME, '{batch}'
                      FROM {STAGE}/{path})
                FILE_FORMAT = (FORMAT_NAME = '{DB}.{SCHEMA}.CSV_FF')
                FORCE = TRUE ON_ERROR = ABORT_STATEMENT""",
        ])
        loaded = hook.get_first(
            f"SELECT COUNT(*) FROM {full} WHERE _BATCH_ID = '{batch}'")[0]
        return {**info, "loaded_rows": loaded}

    @task
    def validate_and_log(result: dict):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        ok = result["source_rows"] == result["loaded_rows"]
        status = "SUCCESS" if ok else "ROW_COUNT_MISMATCH"
        hook.run(
            f"""INSERT INTO {DB}.{SCHEMA}.PIPELINE_RUN_LOG
                (BATCH_ID, TABLE_NAME, SOURCE_ROWS, LOADED_ROWS, STATUS)
                VALUES ('{result["batch"]}', '{result["table"]}',
                        {result["source_rows"]}, {result["loaded_rows"]}, '{status}')""")
        if not ok:
            raise ValueError(
                f"{result['table']}: source={result['source_rows']} "
                f"loaded={result['loaded_rows']}")
        log.info("%s validated: %s rows", result["table"], result["loaded_rows"])

    init = init_log_table()
    uploaded = upload_to_s3.expand(table=list(TABLES))
    loaded = load_bronze.expand(info=uploaded)
    validated = validate_and_log.expand(result=loaded)
    init >> validated


ingestion_dag()