"""Cleaning entrypoint: raw -> cleaned (+ quarantine) for institutions, financials, sod.

Mirrors the backfill entrypoint's shape: discover what raw has, materialize it locally for
Spark, run the per-dataset cleaning function, write results back through the same `ObjectStore`
used for raw. Run with:

    JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run python -m transform.run_cleaning

Reads R2 credentials from the environment, same as ingestion.
"""

from __future__ import annotations

import logging
import tempfile

from dotenv import load_dotenv
from pyspark.sql import functions as F

from ingestion.raw_writer import ObjectStore, R2ObjectStore
from transform.cleaned_branch_deposits import project_branch_deposits
from transform.cleaned_financials import project_financials
from transform.cleaned_scd2 import build_institution_history
from transform.io import (
    dedupe_latest,
    get_spark_session,
    materialize_partitions,
    quarantine_split,
    write_full,
    write_partition,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_INSTITUTION_SNAPSHOT_COLS = ["CERT", "NAME", "CITY", "STALP", "BKCLASS", "REGAGNT", "NAMEHCR"]


def clean_institutions(spark, store: ObjectStore, scratch_dir: str) -> None:
    partitions = store.list_partitions("institutions")
    dest = f"{scratch_dir}/raw/institutions"
    local_dir = materialize_partitions(store, "institutions", partitions, dest)

    # Select only tracked columns first -- avoids mergeSchema choking on unrelated type drift.
    snapshot_frames = []
    for partition in partitions:
        frame = spark.read.parquet(f"{local_dir}/dt={partition}").select(
            *_INSTITUTION_SNAPSHOT_COLS
        )
        snapshot_date = F.to_date(F.lit(partition), "yyyyMMdd")
        snapshot_frames.append(frame.withColumn("SNAPSHOT_DATE", snapshot_date))
    snapshots_df = snapshot_frames[0]
    for frame in snapshot_frames[1:]:
        snapshots_df = snapshots_df.unionByName(frame)

    history_df = build_institution_history(snapshots_df)
    key = write_full(store, "institution_history", history_df, scratch_dir=scratch_dir)
    logger.info("institution_history: wrote %d rows -> %s", history_df.count(), key)


def _read_and_project_partitions(spark, local_dir: str, partitions, project_fn):
    """Projects each partition before unioning -- raw INT/BIGINT type drift across periods
    breaks a single `mergeSchema` read."""
    projected_frames = [
        project_fn(spark.read.parquet(f"{local_dir}/dt={p}")) for p in partitions
    ]
    combined = projected_frames[0]
    for frame in projected_frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _project_dataset(spark, store: ObjectStore, dataset: str, scratch_dir: str, project_fn):
    """Materialize + project every partition of a raw dataset, or `None` if it has none."""
    partitions = store.list_partitions(dataset)
    if not partitions:
        return None
    dest = f"{scratch_dir}/raw/{dataset}"
    local_dir = materialize_partitions(store, dataset, partitions, dest)
    return _read_and_project_partitions(spark, local_dir, partitions, project_fn)


def clean_financials_all(spark, store: ObjectStore, scratch_dir: str) -> None:
    projected = _project_dataset(spark, store, "financials", scratch_dir, project_financials)
    if projected is None:
        raise ValueError("no raw financials partitions to clean")

    # Unioning streamed filings before the dedupe is what makes at-least-once delivery safe.
    streamed = _project_dataset(
        spark, store, "financials_stream", scratch_dir, project_financials
    )
    if streamed is not None:
        logger.info("financials: unioning %d streamed rows before dedupe", streamed.count())
        projected = projected.unionByName(streamed)

    deduped = dedupe_latest(projected, ["cert", "repdte"])
    clean_df, quarantine_df = quarantine_split(deduped, ["cert", "repdte"], ["asset"])
    clean_df.cache()
    quarantine_df.cache()
    _write_by_repdte(store, "cleaned", "financials", clean_df, scratch_dir)
    _write_by_repdte(store, "quarantine", "financials", quarantine_df, scratch_dir)


def clean_branch_deposits_all(spark, store: ObjectStore, scratch_dir: str) -> None:
    projected = _project_dataset(spark, store, "sod", scratch_dir, project_branch_deposits)
    if projected is None:
        raise ValueError("no raw sod partitions to clean")

    key_cols = ["cert", "uninumbr", "repdte"]
    deduped = dedupe_latest(projected, key_cols)
    clean_df, quarantine_df = quarantine_split(deduped, key_cols, ["depsumbr"])
    clean_df.cache()
    quarantine_df.cache()
    _write_by_repdte(store, "cleaned", "branch_deposits", clean_df, scratch_dir)
    _write_by_repdte(store, "quarantine", "branch_deposits", quarantine_df, scratch_dir)


def _write_by_repdte(store: ObjectStore, layer: str, dataset: str, df, scratch_dir: str) -> None:
    repdtes = [row.repdte for row in df.select("repdte").distinct().collect()]
    for repdte in repdtes:
        partition_df = df.filter(F.col("repdte") == repdte)
        key = write_partition(store, layer, dataset, repdte, partition_df, scratch_dir=scratch_dir)
        logger.info(
            "%s/%s dt=%s: wrote %d rows -> %s", layer, dataset, repdte, partition_df.count(), key
        )


def main() -> None:
    load_dotenv()
    store = R2ObjectStore.from_env()
    spark = get_spark_session()
    try:
        with tempfile.TemporaryDirectory() as scratch_dir:
            clean_institutions(spark, store, scratch_dir)
            clean_financials_all(spark, store, scratch_dir)
            clean_branch_deposits_all(spark, store, scratch_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
