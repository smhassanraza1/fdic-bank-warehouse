"""Tests for raw_writer: partition keys, metadata columns, idempotent overwrite, and
tolerance of heterogeneous records (schema evolution)."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.raw_writer import (
    LocalObjectStore,
    build_partition_key,
    unwrap_record,
    write_raw_partition,
)


def test_build_partition_key_is_deterministic_not_a_glob() -> None:
    key = build_partition_key("financials", "20230331")
    assert key == "raw/financials/dt=20230331/data.parquet"


def test_unwrap_record_pulls_data_out_of_api_item() -> None:
    assert unwrap_record({"data": {"CERT": 10042}, "score": 0}) == {"CERT": 10042}


def test_write_raw_partition_adds_metadata_columns(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    result = write_raw_partition(
        store,
        "institutions",
        "20260730",
        [{"CERT": 1, "NAME": "Bank A"}, {"CERT": 2, "NAME": "Bank B"}],
        source_url="https://example.test/institutions",
        batch_id="batch-1",
    )

    assert result.row_count == 2
    table = pq.read_table(tmp_path / result.key)
    assert table.num_rows == 2
    for col in ("_ingested_at", "_source_url", "_batch_id"):
        assert col in table.column_names
    assert table.column("_batch_id").to_pylist() == ["batch-1", "batch-1"]
    assert table.column("_source_url").to_pylist() == ["https://example.test/institutions"] * 2


def test_rerun_overwrites_same_partition_not_a_second_file(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    write_raw_partition(
        store, "institutions", "20260730", [{"CERT": 1}], source_url="https://x", batch_id="b1"
    )
    result = write_raw_partition(
        store,
        "institutions",
        "20260730",
        [{"CERT": 1}, {"CERT": 2}],
        source_url="https://x",
        batch_id="b2",
    )

    partition_dir = tmp_path / "raw" / "institutions" / "dt=20260730"
    files = list(partition_dir.iterdir())
    assert len(files) == 1, "re-run must overwrite the one deterministic key, not accumulate files"

    table = pq.read_table(tmp_path / result.key)
    assert table.num_rows == 2
    assert table.column("_batch_id").to_pylist() == ["b2", "b2"]


def test_heterogeneous_records_union_schema_without_crashing(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    result = write_raw_partition(
        store,
        "institutions",
        "20260730",
        [{"CERT": 1, "NAME": "Bank A"}, {"CERT": 2, "NEW_UPSTREAM_FIELD": "x"}],
        source_url="https://x",
    )

    table = pq.read_table(tmp_path / result.key)
    assert "NEW_UPSTREAM_FIELD" in table.column_names
    assert "NAME" in table.column_names
    assert table.num_rows == 2


def test_empty_records_writes_nothing(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    result = write_raw_partition(store, "institutions", "20260730", [], source_url="https://x")

    assert result.row_count == 0
    assert not (tmp_path / result.key).exists()


def test_get_bytes_round_trips_put_bytes(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    write_raw_partition(
        store, "institutions", "20260730", [{"CERT": 1}], source_url="https://x", batch_id="b1"
    )
    key = build_partition_key("institutions", "20260730")

    table = pq.read_table(pa.BufferReader(store.get_bytes(key)))
    assert table.num_rows == 1


def test_list_partitions_returns_sorted_dt_values(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    for dt in ("20230930", "20230331", "20230630"):
        write_raw_partition(store, "financials", dt, [{"CERT": 1}], source_url="https://x")

    assert store.list_partitions("financials") == ["20230331", "20230630", "20230930"]


def test_list_partitions_empty_for_unknown_dataset(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    assert store.list_partitions("sod") == []
