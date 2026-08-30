from __future__ import annotations

from ingestion.raw_writer import LocalObjectStore
from ingestion.validate_raw import missing_raw_datasets


def test_missing_raw_datasets_empty_when_all_present(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    for dataset in ("institutions", "financials", "sod"):
        store.put_bytes(f"raw/{dataset}/dt=20230331/data.parquet", b"x")

    assert missing_raw_datasets(store) == []


def test_missing_raw_datasets_reports_gaps(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    store.put_bytes("raw/institutions/dt=20230331/data.parquet", b"x")

    assert missing_raw_datasets(store) == ["financials", "sod"]


def test_missing_raw_datasets_empty_store_reports_all() -> None:
    store = LocalObjectStore("/nonexistent")

    assert missing_raw_datasets(store) == ["institutions", "financials", "sod"]
