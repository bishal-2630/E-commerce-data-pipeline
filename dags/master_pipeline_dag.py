import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)


def alert_on_failure(context):
    ti = context["task_instance"]
    log.error("MASTER PIPELINE FAILURE: task=%s", ti.task_id)
    # Later: add email / Slack / Telegram here


def trigger_and_wait(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,   # block until the child DAG finishes
        poke_interval=30,           # check every 30 seconds
        failed_states=["failed"],   # child failed -> this task fails
        retries=1,
    )


@dag(
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params={"simulate_failure": False},
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": alert_on_failure,
    },
    tags=["master", "orchestration"],
)
def master_pipeline():

    @task
    def start():
        log.info("Pipeline started")

    @task(retries=0)
    def failure_demo():
        if get_current_context()["params"].get("simulate_failure"):
            raise RuntimeError("Simulated failure for the demo")
        log.info("No simulated failure requested")

    @task
    def notify_success():
        log.info("Pipeline finished: Bronze -> Silver -> Gold are up to date")

    run_ingestion = trigger_and_wait("run_ingestion", "ingestion_dag")
    run_silver = trigger_and_wait("run_dbt_silver", "dbt_silver")
    run_gold = trigger_and_wait("run_dbt_gold", "dbt_gold")

    start() >> failure_demo() >> run_ingestion >> run_silver >> run_gold >> notify_success()


master_pipeline()