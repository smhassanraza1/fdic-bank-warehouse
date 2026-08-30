"""SCD2 history for institutions: `cleaned_institution_history`.

Institutions has no report-date field on the API — it's a live snapshot,
ingested to raw under `dt=<run_date>`. Each new snapshot may repeat unchanged rows or carry
changed attributes for a `cert`; this module collapses a sequence of snapshots per `cert` into
validity-window rows, closing the old row and opening a new one only where tracked attributes
actually changed.

The very first version of each `cert` gets an open low-water-mark `valid_from`, not the
snapshot date it was first observed at: institutions is currently a single snapshot dated long
after financials data begins (2023Q1), and a fact-to-dim as-of join on
`repdte BETWEEN valid_from AND valid_to` would otherwise match zero historical quarters.
"""

from __future__ import annotations

import hashlib
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

LOW_WATER_MARK = "1900-01-01"
OPEN_VALID_TO = "9999-12-31"

# Raw FDIC field -> canonical cleaned column, tracked for change detection.
TRACKED_ATTRS = {
    "NAME": "name",
    "CITY": "city",
    "STALP": "state",
    "BKCLASS": "bkclass",
    "REGAGNT": "regagnt",
    "NAMEHCR": "holding_company",
}


def institution_row_hash(row: dict[str, Any]) -> str:
    """Hash the tracked attributes of one institution row, keyed by raw FDIC field name
    (`NAME`, `CITY`, ...). Order-independent of input dict key order and null-normalized so
    `None`/missing/empty compare equal."""
    parts = [str(row.get(col) or "") for col in TRACKED_ATTRS]
    canonical = "|".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _institution_row_hash_udf(row: Any) -> str:
    """UDF adapter: `F.struct(...)` delivers a `pyspark.sql.Row`, not a plain dict."""
    return institution_row_hash(row.asDict())


_row_hash_udf = F.udf(_institution_row_hash_udf, StringType())


def build_institution_history(
    snapshots_df: DataFrame, low_water_mark: str = LOW_WATER_MARK
) -> DataFrame:
    """Collapse `(cert, snapshot_date)` institution snapshots into SCD2 validity windows.

    `snapshots_df` must have `CERT`, `SNAPSHOT_DATE` (a date-typed column) and the raw
    `TRACKED_ATTRS` keys. Returns `cert, name, city, state, bkclass, regagnt, holding_company,
    valid_from, valid_to, is_current, _hash`.
    """
    hashed_raw = snapshots_df.withColumn(
        "_hash",
        _row_hash_udf(F.struct(*(F.col(raw).alias(raw) for raw in TRACKED_ATTRS))),
    )

    hashed = hashed_raw.select(
        F.col("CERT").cast("long").alias("cert"),
        F.col("SNAPSHOT_DATE").alias("snapshot_date"),
        *(F.col(raw).alias(clean) for raw, clean in TRACKED_ATTRS.items()),
        "_hash",
    )

    cert_order = Window.partitionBy("cert").orderBy("snapshot_date")
    prev_hash = F.lag("_hash").over(cert_order)
    is_new_group = F.when(prev_hash.isNull() | (F.col("_hash") != prev_hash), 1).otherwise(0)
    grouped = hashed.withColumn("_group_id", F.sum(is_new_group).over(cert_order))

    group_cols = ["cert", "_group_id", *TRACKED_ATTRS.values(), "_hash"]
    groups = grouped.groupBy(*group_cols).agg(F.min("snapshot_date").alias("group_start"))

    group_order = Window.partitionBy("cert").orderBy("group_start")
    with_bounds = groups.withColumn(
        "valid_to_date", F.date_sub(F.lead("group_start").over(group_order), 1)
    ).withColumn("_rank", F.row_number().over(group_order))

    result = with_bounds.select(
        "cert",
        *TRACKED_ATTRS.values(),
        F.when(F.col("_rank") == 1, F.lit(low_water_mark))
        .otherwise(F.date_format("group_start", "yyyy-MM-dd"))
        .alias("valid_from"),
        F.when(F.col("valid_to_date").isNull(), F.lit(OPEN_VALID_TO))
        .otherwise(F.date_format("valid_to_date", "yyyy-MM-dd"))
        .alias("valid_to"),
        (F.col("valid_to_date").isNull()).alias("is_current"),
        "_hash",
    )
    return result
