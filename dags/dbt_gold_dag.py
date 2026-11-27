from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile = ProfileConfig(
    profile_name="olist",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"database": "ECOM", "schema": "GOLD"},
    ),
)

dbt_gold = DbtDag(
    dag_id="dbt_gold",
    project_config=ProjectConfig("/usr/local/airflow/include/dbt_project"),
    profile_config=profile,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"),
    render_config=RenderConfig(select=["path:models/gold"]),
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2},
)