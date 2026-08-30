"""Publishes a single-row pipeline freshness marker to BigQuery — the final DAG arrow.

`GET /health` reads this table for warehouse freshness instead of scanning
`fct_bank_quarterly_financials` on every request. One row, rewritten wholesale on every run,
so it always reflects the most recent successful pipeline run -- no history, no per-run state.

BigQuery Sandbox rejects all DML (no billing account), so this reuses `bq_load`'s load-job
helper (`WRITE_TRUNCATE`) instead of an INSERT/UPDATE statement.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pyarrow as pa
from google.cloud import bigquery

from loaders.bq_load import BigQueryConfig, ensure_table, load_table

logger = logging.getLogger(__name__)

# No underscore: BigQuery hides underscore-prefixed names in the console UI.
FRESHNESS_TABLE = "pipeline_freshness"

FRESHNESS_SCHEMA = [
    bigquery.SchemaField("published_at", "TIMESTAMP"),
    bigquery.SchemaField("max_repdte", "DATE"),
    bigquery.SchemaField("financials_row_count", "INT64"),
]


@dataclass(frozen=True)
class FreshnessMetric:
    published_at: datetime
    max_repdte: date | None
    financials_row_count: int


def compute_freshness(client: bigquery.Client, config: BigQueryConfig) -> FreshnessMetric:
    query = f"""
        select max(repdte) as max_repdte, count(*) as row_count
        from `{config.table_ref("fct_bank_quarterly_financials")}`
    """
    row = next(iter(client.query(query).result()))
    return FreshnessMetric(
        published_at=datetime.now(UTC),
        max_repdte=row.max_repdte,
        financials_row_count=row.row_count or 0,
    )


def publish_freshness(client: bigquery.Client, config: BigQueryConfig) -> FreshnessMetric:
    table_ref = config.table_ref(FRESHNESS_TABLE)
    ensure_table(client, table_ref, FRESHNESS_SCHEMA)

    metric = compute_freshness(client, config)
    table = pa.table(
        {
            "published_at": pa.array(
                [metric.published_at], type=pa.timestamp("us", tz="UTC")
            ),
            "max_repdte": pa.array([metric.max_repdte], type=pa.date32()),
            "financials_row_count": pa.array(
                [metric.financials_row_count], type=pa.int64()
            ),
        }
    )
    load_table(client, table, table_ref, FRESHNESS_SCHEMA)
    logger.info("published freshness metric -> %s: %s", table_ref, metric)
    return metric
