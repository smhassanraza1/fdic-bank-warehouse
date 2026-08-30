"""Typed, deduped financials: `cleaned_financials`.

Raw financials carries the FDIC API's default ~164-column response, not the full 2378-field
`financials.json` schema. Rather than casting everything, this projects an explicit allowlist
sized to what the curated fact and marts need — an unknown upstream column added later simply
isn't selected, so schema evolution can't break this job.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transform.io import dedupe_latest, quarantine_split

KEY_COLS = ["cert", "repdte"]

# Raw FDIC field -> (cleaned column, Spark cast type).
FINANCIALS_COLUMNS: dict[str, str] = {
    "CERT": "long",
    "REPDTE": "string",
    "NAME": "string",
    "STALP": "string",
    "STNAME": "string",
    "BKCLASS": "string",
    "ASSET": "double",
    "DEP": "double",
    "EQ": "double",
    "NETINC": "double",
    "ROA": "double",
    "ROE": "double",
}


def project_financials(raw_df: DataFrame) -> DataFrame:
    """Select + cast the financials allowlist. Kept separate from `clean_financials` so callers
    can cast each raw partition individually before unioning them."""
    return raw_df.select(
        *(F.col(raw).cast(cast).alias(raw.lower()) for raw, cast in FINANCIALS_COLUMNS.items()),
        "_ingested_at",
        "_batch_id",
    )


def clean_financials(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Project + cast the financials allowlist, dedupe on `(cert, repdte)`, and split out
    quarantined rows (missing key, negative `asset`). Returns `(clean, quarantine)`."""
    deduped = dedupe_latest(project_financials(raw_df), KEY_COLS)
    clean, quarantine = quarantine_split(deduped, KEY_COLS, non_negative_cols=["asset"])
    return clean, quarantine
