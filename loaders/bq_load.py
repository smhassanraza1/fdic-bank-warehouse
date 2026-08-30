"""Loads cleaned parquet (R2) into BigQuery — the CLEANED -> BigQuery-load-job arrow.

BigQuery Sandbox has no streaming inserts, so every write here is a batch load
job (`client.load_table_from_file`), never `insert_rows`.

All three tables are rewritten wholesale (`WRITE_TRUNCATE` on the whole table) every run, never
partitioned by `repdte`: Sandbox mode hard-caps expiration at 60 days, and `repdte` is a
historical business date (2023-2025) that would make BigQuery treat those partitions as already
past expiry and delete them right after load. A table's own 60-day expiration is set once at
creation and `WRITE_TRUNCATE` doesn't renew it, so each table still self-deletes 60 days after
its first load -- expected in Sandbox, and self-healing as long as this runs again before then.

Run with:

    uv run python -m loaders.bq_load

Reads BigQuery + R2 credentials from the environment.
"""

from __future__ import annotations

import io
import logging
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ingestion.raw_writer import ObjectStore, R2ObjectStore
from loaders.config import BigQueryConfig  # re-exported: importers predate the split

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INSTITUTION_HISTORY_DATASET = "institution_history"
FINANCIALS_DATASET = "financials"
BRANCH_DEPOSITS_DATASET = "branch_deposits"

INSTITUTION_HISTORY_TABLE = "cleaned_institution_history"
FINANCIALS_TABLE = "cleaned_financials"
BRANCH_DEPOSITS_TABLE = "cleaned_branch_deposits"

INSTITUTION_HISTORY_SCHEMA = [
    bigquery.SchemaField("cert", "INT64"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("bkclass", "STRING"),
    bigquery.SchemaField("regagnt", "STRING"),
    bigquery.SchemaField("holding_company", "STRING"),
    bigquery.SchemaField("valid_from", "DATE"),
    bigquery.SchemaField("valid_to", "DATE"),
    bigquery.SchemaField("is_current", "BOOL"),
    bigquery.SchemaField("_hash", "STRING"),
]

FINANCIALS_SCHEMA = [
    bigquery.SchemaField("cert", "INT64"),
    bigquery.SchemaField("repdte", "DATE"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("stalp", "STRING"),
    bigquery.SchemaField("stname", "STRING"),
    bigquery.SchemaField("bkclass", "STRING"),
    bigquery.SchemaField("asset", "FLOAT64"),
    bigquery.SchemaField("dep", "FLOAT64"),
    bigquery.SchemaField("eq", "FLOAT64"),
    bigquery.SchemaField("netinc", "FLOAT64"),
    bigquery.SchemaField("roa", "FLOAT64"),
    bigquery.SchemaField("roe", "FLOAT64"),
    bigquery.SchemaField("_ingested_at", "STRING"),
    bigquery.SchemaField("_batch_id", "STRING"),
]

BRANCH_DEPOSITS_SCHEMA = [
    bigquery.SchemaField("cert", "INT64"),
    bigquery.SchemaField("uninumbr", "INT64"),
    bigquery.SchemaField("namefull", "STRING"),
    bigquery.SchemaField("citybr", "STRING"),
    bigquery.SchemaField("stalpbr", "STRING"),
    bigquery.SchemaField("stnamebr", "STRING"),
    bigquery.SchemaField("depsumbr", "FLOAT64"),
    bigquery.SchemaField("asset", "FLOAT64"),
    bigquery.SchemaField("year", "INT64"),
    bigquery.SchemaField("repdte", "DATE"),
    bigquery.SchemaField("_ingested_at", "STRING"),
    bigquery.SchemaField("_batch_id", "STRING"),
]


def _reformat_column_to_date(table: pa.Table, column: str, fmt: str) -> pa.Table:
    """Parse a string column into `date32`. BigQuery's Parquet loader has no STRING->DATE
    coercion -- Parquet stores DATE as a binary int32, so the column must already be
    date-typed before it reaches the load job, not just an ISO-8601-looking string."""
    idx = table.schema.get_field_index(column)
    parsed = pc.strptime(table.column(column), format=fmt, unit="s")
    as_date = pc.cast(parsed, pa.date32())
    return table.set_column(idx, column, as_date)


def reformat_repdte_to_date(table: pa.Table) -> pa.Table:
    """`repdte` arrives as `yyyyMMdd` (the cleaned layer's raw FDIC format)."""
    return _reformat_column_to_date(table, "repdte", "%Y%m%d")


def reformat_validity_window_to_date(table: pa.Table) -> pa.Table:
    """`valid_from`/`valid_to` arrive as ISO-8601 strings (`yyyy-MM-dd`)."""
    table = _reformat_column_to_date(table, "valid_from", "%Y-%m-%d")
    return _reformat_column_to_date(table, "valid_to", "%Y-%m-%d")


def ensure_dataset(client: bigquery.Client, config: BigQueryConfig) -> None:
    try:
        client.get_dataset(config.dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(config.dataset_ref)
        dataset.location = config.location
        client.create_dataset(dataset)
        logger.info("created dataset %s", config.dataset_ref)


def ensure_table(
    client: bigquery.Client, table_ref: str, schema: list[bigquery.SchemaField]
) -> None:
    try:
        client.get_table(table_ref)
    except NotFound:
        client.create_table(bigquery.Table(table_ref, schema=schema))
        logger.info("created table %s", table_ref)


def load_table(
    client: bigquery.Client,
    table: pa.Table,
    destination: str,
    schema: list[bigquery.SchemaField],
) -> int:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(buf.getvalue())
        tmp.flush()
        with open(tmp.name, "rb") as f:
            job = client.load_table_from_file(f, destination, job_config=job_config)
        job.result()
    return job.output_rows


def _read_cleaned_partitions(store: ObjectStore, dataset: str) -> pa.Table:
    partitions = store.list_partitions(dataset, layer="cleaned")
    if not partitions:
        raise RuntimeError(
            f"no cleaned/{dataset} partitions found -- run spark_clean before load_bigquery"
        )
    tables = [
        pq.read_table(pa.BufferReader(store.get_bytes(f"cleaned/{dataset}/dt={repdte}/data.parquet")))
        for repdte in partitions
    ]
    return pa.concat_tables(tables)


def load_institution_history(
    client: bigquery.Client, store: ObjectStore, config: BigQueryConfig
) -> None:
    table_ref = config.table_ref(INSTITUTION_HISTORY_TABLE)
    ensure_table(client, table_ref, INSTITUTION_HISTORY_SCHEMA)

    key = f"cleaned/{INSTITUTION_HISTORY_DATASET}/data.parquet"
    raw_table = pq.read_table(pa.BufferReader(store.get_bytes(key)))
    reformatted = reformat_validity_window_to_date(raw_table)
    rows = load_table(client, reformatted, table_ref, INSTITUTION_HISTORY_SCHEMA)
    logger.info("%s: loaded %d rows -> %s", INSTITUTION_HISTORY_TABLE, rows, table_ref)


def load_full_table(
    client: bigquery.Client,
    store: ObjectStore,
    config: BigQueryConfig,
    *,
    dataset: str,
    table: str,
    schema: list[bigquery.SchemaField],
) -> None:
    table_ref = config.table_ref(table)
    ensure_table(client, table_ref, schema)

    combined = _read_cleaned_partitions(store, dataset)
    reformatted = reformat_repdte_to_date(combined)
    rows = load_table(client, reformatted, table_ref, schema)
    logger.info("%s: loaded %d rows -> %s", table, rows, table_ref)


def main() -> None:
    # override=True: an ambient GOOGLE_APPLICATION_CREDENTIALS from elsewhere must not win.
    load_dotenv(override=True)
    config = BigQueryConfig.from_env()
    client = bigquery.Client(project=config.project, location=config.location)
    store = R2ObjectStore.from_env()

    ensure_dataset(client, config)
    load_institution_history(client, store, config)
    load_full_table(
        client,
        store,
        config,
        dataset=FINANCIALS_DATASET,
        table=FINANCIALS_TABLE,
        schema=FINANCIALS_SCHEMA,
    )
    load_full_table(
        client,
        store,
        config,
        dataset=BRANCH_DEPOSITS_DATASET,
        table=BRANCH_DEPOSITS_TABLE,
        schema=BRANCH_DEPOSITS_SCHEMA,
    )


if __name__ == "__main__":
    main()
