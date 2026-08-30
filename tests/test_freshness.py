from __future__ import annotations

import datetime

from loaders.bq_load import BigQueryConfig
from loaders.freshness import compute_freshness


class _FakeRow:
    def __init__(self, max_repdte, row_count) -> None:
        self.max_repdte = max_repdte
        self.row_count = row_count


class _FakeQueryJob:
    def __init__(self, row: _FakeRow) -> None:
        self._row = row

    def result(self):
        return iter([self._row])


class _FakeClient:
    def __init__(self, row: _FakeRow) -> None:
        self._row = row
        self.queries: list[str] = []

    def query(self, sql: str) -> _FakeQueryJob:
        self.queries.append(sql)
        return _FakeQueryJob(self._row)


def test_compute_freshness_reads_max_repdte_and_row_count() -> None:
    row = _FakeRow(max_repdte=datetime.date(2025, 12, 31), row_count=54000)
    client = _FakeClient(row)
    config = BigQueryConfig(project="proj", dataset="fig_gold")

    metric = compute_freshness(client, config)

    assert metric.max_repdte == datetime.date(2025, 12, 31)
    assert metric.financials_row_count == 54000
    assert "fct_bank_quarterly_financials" in client.queries[0]


def test_compute_freshness_handles_empty_fact_table() -> None:
    row = _FakeRow(max_repdte=None, row_count=None)
    client = _FakeClient(row)
    config = BigQueryConfig(project="proj", dataset="fig_gold")

    metric = compute_freshness(client, config)

    assert metric.max_repdte is None
    assert metric.financials_row_count == 0
