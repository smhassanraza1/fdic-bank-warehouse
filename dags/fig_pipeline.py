"""fig_pipeline DAG.

ingest_api -> validate_raw -> spark_clean -> load_bigquery -> dbt_run -> dbt_test
  -> publish_freshness_metric

Every step overwrites its layer wholesale -- raw is a partition overwrite, cleaned/curated are
full-table rebuilds (BigQuery Sandbox has no billing account, so incremental MERGE/DML is
rejected outright). That's what makes the DAG idempotent: re-running any task, or the whole DAG,
for the same period produces an identical result, no per-run state to reconcile. `dbt_test` gates
`publish_freshness_metric` -- bad data never gets marked fresh.

The `start_repdte`/`end_repdte` params narrow `ingest_api`'s FDIC fetch to a sub-range for a
partial backfill -- downstream tasks still rebuild cleaned/curated wholesale regardless of the
range ingested, since Sandbox mode rejects incremental DML.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/project/dbt"


@dag(
    dag_id="fig_pipeline",
    description="FDIC filings: raw -> cleaned -> curated -> freshness metric",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    params={"start_repdte": "20230331", "end_repdte": "20251231"},
    tags=["fig"],
)
def fig_pipeline() -> None:
    @task
    def ingest_api(**context) -> None:
        from ingestion.run_backfill import main as run_backfill

        params = context["params"]
        run_backfill(params["start_repdte"], params["end_repdte"])

    @task
    def validate_raw() -> None:
        from dotenv import load_dotenv

        from ingestion.raw_writer import R2ObjectStore
        from ingestion.validate_raw import missing_raw_datasets

        load_dotenv(override=True)
        store = R2ObjectStore.from_env()
        missing = missing_raw_datasets(store)
        if missing:
            raise AirflowFailException(f"raw layer missing partitions for: {missing}")

    @task
    def spark_clean() -> None:
        from transform.run_cleaning import main as run_cleaning

        run_cleaning()

    @task
    def load_bigquery() -> None:
        from loaders.bq_load import main as load_bq

        load_bq()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --target dev",
        cwd=DBT_PROJECT_DIR,
    )

    # retries=0: a dbt test failure is deterministic, retrying only delays it.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --target dev",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    @task
    def publish_freshness_metric() -> None:
        from dotenv import load_dotenv
        from google.cloud import bigquery

        from loaders.bq_load import BigQueryConfig
        from loaders.freshness import publish_freshness

        load_dotenv(override=True)
        config = BigQueryConfig.from_env()
        client = bigquery.Client(project=config.project, location=config.location)
        publish_freshness(client, config)

    (
        ingest_api()
        >> validate_raw()
        >> spark_clean()
        >> load_bigquery()
        >> dbt_run
        >> dbt_test
        >> publish_freshness_metric()
    )


fig_pipeline()
