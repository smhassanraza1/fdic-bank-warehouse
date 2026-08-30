"""Shared plumbing for the cleaning layer: Spark session, raw materialization, cleaned/
quarantine writes.

Spark reads/writes local parquet only — no `s3a`/hadoop-aws wiring. Raw bytes are pulled
from the `ObjectStore` (R2 or local) into a local scratch tree that mirrors the `dt=...`
partition layout, and cleaned/quarantine output is written back the same way, reusing the
existing byte-oriented store rather than adding a second cloud-storage path.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.raw_writer import ObjectStore


def get_spark_session(app_name: str = "fig-cleaning") -> SparkSession:
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def materialize_partitions(
    store: ObjectStore, dataset: str, partitions: Iterable[str], dest_dir: str
) -> str:
    """Pull raw `dt=...` partitions for `dataset` into `dest_dir`, mirroring the raw key layout.

    Every parquet file under a partition is pulled — a streaming consumer adds one per flush.

    Returns `dest_dir` so callers can pass it straight to `spark.read.parquet`.
    """
    for partition in partitions:
        for key in store.list_partition_keys(dataset, partition):
            data = store.get_bytes(key)
            out_path = os.path.join(dest_dir, f"dt={partition}", os.path.basename(key))
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(data)
    return dest_dir


def write_partition(
    store: ObjectStore,
    layer: str,
    dataset: str,
    partition: str,
    df: DataFrame,
    *,
    scratch_dir: str,
) -> str:
    """Write one `{layer}/{dataset}/dt={partition}/data.parquet` via the object store.

    `layer` is `"cleaned"` or `"quarantine"`. Spark writes a single local parquet file first
    (`coalesce(1)`), then its bytes are pushed through `store.put_bytes` — the same
    partition-overwrite convention as raw.
    """
    local_dir = os.path.join(scratch_dir, layer, dataset, f"dt={partition}")
    df.coalesce(1).write.mode("overwrite").parquet(local_dir)
    part_file = _find_single_part_file(local_dir)
    with open(part_file, "rb") as f:
        data = f.read()
    key = f"{layer}/{dataset}/dt={partition}/data.parquet"
    store.put_bytes(key, data)
    return key


def write_full(store: ObjectStore, dataset: str, df: DataFrame, *, scratch_dir: str) -> str:
    """Write a non-partitioned cleaned dataset (e.g. `cleaned_institution_history`), rewritten
    wholesale on every run."""
    local_dir = os.path.join(scratch_dir, "cleaned", dataset)
    df.coalesce(1).write.mode("overwrite").parquet(local_dir)
    part_file = _find_single_part_file(local_dir)
    with open(part_file, "rb") as f:
        data = f.read()
    key = f"cleaned/{dataset}/data.parquet"
    store.put_bytes(key, data)
    return key


def dedupe_latest(df: DataFrame, key_cols: list[str]) -> DataFrame:
    """Keep one row per `key_cols`: latest `_ingested_at`, `_batch_id` as a stable tiebreak so
    a rerun over identical raw input always keeps the same survivor — rerunning a period must
    produce zero *changed* rows, not just zero duplicates."""
    order = Window.partitionBy(*key_cols).orderBy(
        F.col("_ingested_at").desc(), F.col("_batch_id").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(order))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_split(
    df: DataFrame, key_cols: list[str], non_negative_cols: list[str]
) -> tuple[DataFrame, DataFrame]:
    """Split `df` into (clean, quarantine) rows. Quarantines null `key_cols` or a negative value
    in any `non_negative_cols`, tagging the reason rather than dropping the row silently."""
    missing_key = F.lit(False)
    for col in key_cols:
        missing_key = missing_key | F.col(col).isNull()

    negative = F.lit(False)
    for col in non_negative_cols:
        negative = negative | (F.col(col) < 0)

    reason = (
        F.when(missing_key, F.lit("missing_key"))
        .when(negative, F.lit("negative_value"))
        .otherwise(F.lit(None))
    )
    tagged = df.withColumn("_reject_reason", reason)
    quarantine = tagged.filter(F.col("_reject_reason").isNotNull())
    clean = tagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    return clean, quarantine


def _find_single_part_file(local_dir: str) -> str:
    for name in os.listdir(local_dir):
        if name.startswith("part-") and name.endswith(".parquet"):
            return os.path.join(local_dir, name)
    raise FileNotFoundError(f"no part-*.parquet file found in {local_dir}")
