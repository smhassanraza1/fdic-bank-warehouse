"""Backfill entrypoint: pull institutions, financials, and /sod into raw.

Covers 12 quarters (2023Q1-2025Q4) across ~4,500 active institutions by default. Run with:

    uv run python -m ingestion.run_backfill

`main(start_repdte, end_repdte)` narrows the FDIC fetch to a sub-range of that window --
useful for a partial re-run without re-pulling everything. Downstream (spark_clean,
load_bigquery) still rebuilds cleaned/curated wholesale regardless of the range ingested here:
BigQuery Sandbox has no billing account and rejects incremental DML outright, so a windowed
rebuild there would be incorrect, not just slower.

Reads FDIC_API_BASE and the R2_* credentials from the environment.
Each dataset partitions differently because the source endpoints don't share a grain:

- `institutions` has no report-date field — it's a live snapshot, so it's partitioned by the
  date the backfill ran (`dt=<run_date>`), not by financial period.
- `financials` is one partition per quarter-end (`REPDTE`), giving one raw file per quarter.
- `sod` (branch deposits) is filed annually (`YEAR`), so one partition per calendar year.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from dotenv import load_dotenv

from ingestion.fdic_client import FdicClient
from ingestion.raw_writer import ObjectStore, R2ObjectStore, unwrap_record, write_raw_partition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

QUARTER_ENDS = [
    "20230331",
    "20230630",
    "20230930",
    "20231231",
    "20240331",
    "20240630",
    "20240930",
    "20241231",
    "20250331",
    "20250630",
    "20250930",
    "20251231",
]
SOD_YEARS = [2023, 2024, 2025]


def quarters_in_range(start_repdte: str, end_repdte: str) -> list[str]:
    return [q for q in QUARTER_ENDS if start_repdte <= q <= end_repdte]


def years_in_range(start_repdte: str, end_repdte: str) -> list[int]:
    start_year, end_year = int(start_repdte[:4]), int(end_repdte[:4])
    return [year for year in SOD_YEARS if start_year <= year <= end_year]


def ingest_institutions(client: FdicClient, store: ObjectStore, run_date: str) -> None:
    filters = "ACTIVE:1"
    source_url = f"{client.base_url}institutions?filters={filters}"
    records = [unwrap_record(item) for item in client.get_institutions(filters=filters)]
    result = write_raw_partition(
        store, "institutions", run_date, records, source_url=source_url
    )
    logger.info("institutions dt=%s: wrote %d rows -> %s", run_date, result.row_count, result.key)


def ingest_financials(
    client: FdicClient, store: ObjectStore, quarter_ends: list[str] = QUARTER_ENDS
) -> None:
    for quarter_end in quarter_ends:
        filters = f"REPDTE:{quarter_end}"
        source_url = f"{client.base_url}financials?filters={filters}"
        records = [unwrap_record(item) for item in client.get_financials(filters=filters)]
        result = write_raw_partition(
            store, "financials", quarter_end, records, source_url=source_url
        )
        logger.info(
            "financials dt=%s: wrote %d rows -> %s", quarter_end, result.row_count, result.key
        )


def ingest_sod(client: FdicClient, store: ObjectStore, years: list[int] = SOD_YEARS) -> None:
    for year in years:
        filters = f"YEAR:{year}"
        source_url = f"{client.base_url}sod?filters={filters}"
        records = [unwrap_record(item) for item in client.get_sod(filters=filters)]
        result = write_raw_partition(store, "sod", str(year), records, source_url=source_url)
        logger.info("sod dt=%s: wrote %d rows -> %s", year, result.row_count, result.key)


def main(start_repdte: str = QUARTER_ENDS[0], end_repdte: str = QUARTER_ENDS[-1]) -> None:
    load_dotenv()
    base_url = os.environ["FDIC_API_BASE"]
    run_date = datetime.now(UTC).strftime("%Y%m%d")

    with FdicClient(base_url) as client:
        store = R2ObjectStore.from_env()
        ingest_institutions(client, store, run_date)
        ingest_financials(client, store, quarters_in_range(start_repdte, end_repdte))
        ingest_sod(client, store, years_in_range(start_repdte, end_repdte))


if __name__ == "__main__":
    main()
