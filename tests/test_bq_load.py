from __future__ import annotations

import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ingestion.raw_writer import LocalObjectStore
from loaders.bq_load import (
    BigQueryConfig,
    _read_cleaned_partitions,
    ensure_dataset,
    ensure_table,
    reformat_repdte_to_date,
    reformat_validity_window_to_date,
)


class _FakeClient:
    """Minimal double for the bits of `bigquery.Client` `ensure_dataset`/`ensure_table` use."""

    def __init__(self, *, existing: bool) -> None:
        self._existing = existing
        self.created_datasets: list[bigquery.Dataset] = []
        self.created_tables: list[bigquery.Table] = []

    def get_dataset(self, ref):
        if not self._existing:
            raise NotFound("no such dataset")
        return ref

    def create_dataset(self, dataset):
        self.created_datasets.append(dataset)

    def get_table(self, ref):
        if not self._existing:
            raise NotFound("no such table")
        return ref

    def create_table(self, table):
        self.created_tables.append(table)


def test_reformat_repdte_to_date_parses_yyyymmdd() -> None:
    table = pa.table({"cert": [1, 2], "repdte": ["20230331", "20230630"]})

    result = reformat_repdte_to_date(table)

    assert result.column("repdte").type == pa.date32()
    assert result.column("repdte").to_pylist() == [
        datetime.date(2023, 3, 31),
        datetime.date(2023, 6, 30),
    ]


def test_reformat_validity_window_to_date_parses_iso_dates() -> None:
    table = pa.table(
        {
            "cert": [1, 2],
            "valid_from": ["1900-01-01", "2023-06-30"],
            "valid_to": ["9999-12-31", "2024-01-01"],
        }
    )

    result = reformat_validity_window_to_date(table)

    assert result.column("valid_from").type == pa.date32()
    assert result.column("valid_to").type == pa.date32()
    assert result.column("valid_from").to_pylist() == [
        datetime.date(1900, 1, 1),
        datetime.date(2023, 6, 30),
    ]
    assert result.column("valid_to").to_pylist() == [
        datetime.date(9999, 12, 31),
        datetime.date(2024, 1, 1),
    ]


def test_read_cleaned_partitions_unions_all_repdte_partitions(tmp_path) -> None:
    store = LocalObjectStore(str(tmp_path))
    for repdte, cert in [("20230331", 1), ("20230630", 2)]:
        buf = pa.BufferOutputStream()
        pq.write_table(pa.table({"cert": [cert], "repdte": [repdte]}), buf)
        store.put_bytes(f"cleaned/financials/dt={repdte}/data.parquet", buf.getvalue().to_pybytes())

    combined = _read_cleaned_partitions(store, "financials")

    assert combined.num_rows == 2
    assert sorted(combined.column("repdte").to_pylist()) == ["20230331", "20230630"]


def test_bigquery_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("BQ_PROJECT", "my-project")
    monkeypatch.setenv("BQ_DATASET", "fig_gold")
    monkeypatch.setenv("BQ_LOCATION", "US")

    config = BigQueryConfig.from_env()

    assert config.dataset_ref == "my-project.fig_gold"
    assert config.table_ref("cleaned_financials") == "my-project.fig_gold.cleaned_financials"


def test_bigquery_config_from_env_requires_project(monkeypatch) -> None:
    monkeypatch.delenv("BQ_PROJECT", raising=False)
    with pytest.raises(KeyError):
        BigQueryConfig.from_env()


def test_ensure_dataset_creates_when_missing() -> None:
    client = _FakeClient(existing=False)
    config = BigQueryConfig(project="proj", dataset="fig_gold", location="US")

    ensure_dataset(client, config)

    assert len(client.created_datasets) == 1
    assert client.created_datasets[0].location == "US"


def test_ensure_dataset_noop_when_present() -> None:
    client = _FakeClient(existing=True)
    config = BigQueryConfig(project="proj", dataset="fig_gold")

    ensure_dataset(client, config)

    assert client.created_datasets == []


def test_ensure_table_creates_table_when_missing() -> None:
    client = _FakeClient(existing=False)
    schema = [bigquery.SchemaField("cert", "INT64")]

    ensure_table(client, "proj.fig_gold.cleaned_financials", schema)

    assert len(client.created_tables) == 1


def test_ensure_table_noop_when_present() -> None:
    client = _FakeClient(existing=True)
    schema = [bigquery.SchemaField("cert", "INT64")]

    ensure_table(client, "proj.fig_gold.cleaned_institution_history", schema)

    assert client.created_tables == []
