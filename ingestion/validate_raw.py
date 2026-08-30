"""Sanity check the raw layer before cleaning starts.

Cheap guardrail against silent data loss: if `ingest_api` silently wrote
zero partitions for a dataset (FDIC API outage, a filters typo), fail loudly here instead of
letting `spark_clean` run on an empty or missing partition set.
"""

from __future__ import annotations

from ingestion.raw_writer import ObjectStore

REQUIRED_DATASETS = ("institutions", "financials", "sod")


def missing_raw_datasets(
    store: ObjectStore, datasets: tuple[str, ...] = REQUIRED_DATASETS
) -> list[str]:
    return [dataset for dataset in datasets if not store.list_partitions(dataset)]
