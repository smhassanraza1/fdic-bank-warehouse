"""Branch-level deposits from `/sod`: `cleaned_branch_deposits`.

Raw `sod` has no report-date field, only `YEAR` — the FDIC Summary of Deposits survey date is
always June 30, so `repdte` is derived as `f"{YEAR}0630"` rather than read off a raw column.
Branch-level state/deposit fields (`STALPBR`/`STNAMEBR`/`DEPSUMBR`) are used, not the
headquarters-level `STALP`/`STNAME`/`DEPSUM` — this is the volume driver for
`mart_deposit_concentration`, so grain is `(cert, uninumbr, repdte)`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transform.io import dedupe_latest, quarantine_split

KEY_COLS = ["cert", "uninumbr", "repdte"]

# Raw FDIC field -> (cleaned column, Spark cast type). REPDTE is derived, not a raw column.
SOD_COLUMNS: dict[str, str] = {
    "CERT": "long",
    "UNINUMBR": "long",
    "NAMEFULL": "string",
    "CITYBR": "string",
    "STALPBR": "string",
    "STNAMEBR": "string",
    "DEPSUMBR": "double",
    "ASSET": "double",
    "YEAR": "long",
}


def project_branch_deposits(raw_df: DataFrame) -> DataFrame:
    """Select + cast the sod allowlist and derive `repdte` from `YEAR`. Kept separate from
    `clean_branch_deposits` so callers can cast each raw partition individually before unioning."""
    return raw_df.select(
        *(F.col(raw).cast(cast).alias(raw.lower()) for raw, cast in SOD_COLUMNS.items()),
        "_ingested_at",
        "_batch_id",
    ).withColumn("repdte", F.concat(F.col("year").cast("string"), F.lit("0630")))


def clean_branch_deposits(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Project + cast the sod allowlist, derive `repdte` from `YEAR`, dedupe on
    `(cert, uninumbr, repdte)`, and split out quarantined rows (missing key, negative deposits).
    Returns `(clean, quarantine)`."""
    deduped = dedupe_latest(project_branch_deposits(raw_df), KEY_COLS)
    clean, quarantine = quarantine_split(deduped, KEY_COLS, non_negative_cols=["depsumbr"])
    return clean, quarantine
