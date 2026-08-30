"""API tests.

No network and no BigQuery: a fake client returns canned rows and records the SQL and
parameters it was handed. The query builders are pure functions, so what they emit is
asserted directly.
"""

from __future__ import annotations

import datetime

import pytest
from fastapi.testclient import TestClient
from google.api_core.exceptions import NotFound

from api import queries
from api.deps import get_client, get_config
from api.main import app
from api.models import Metric
from loaders.config import BigQueryConfig

CONFIG = BigQueryConfig(project="proj", dataset="fig_gold")


class _FakeQueryJob:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def result(self):
        return iter(self._rows)


class _FakeClient:
    """Returns `rows` for every query; raises `error` instead if one is given."""

    def __init__(self, rows: list[dict] | None = None, error: Exception | None = None) -> None:
        self.rows = rows or []
        self.error = error
        self.queries: list[str] = []
        self.params: list[list] = []

    def query(self, sql: str, job_config=None) -> _FakeQueryJob:
        self.queries.append(sql)
        self.params.append(list(job_config.query_parameters) if job_config else [])
        if self.error:
            raise self.error
        return _FakeQueryJob(self.rows)


@pytest.fixture
def make_client():
    created: list[_FakeClient] = []

    def _make(rows=None, error=None) -> TestClient:
        fake = _FakeClient(rows=rows, error=error)
        created.append(fake)
        app.dependency_overrides[get_client] = lambda: fake
        app.dependency_overrides[get_config] = lambda: CONFIG
        client = TestClient(app)
        client.fake = fake
        return client

    yield _make
    app.dependency_overrides.clear()


def _params(fake: _FakeClient) -> dict:
    return {p.name: p.value for p in fake.params[0]}


# --- query builders ---------------------------------------------------------------------


def test_institutions_query_excludes_the_unknown_dim_member() -> None:
    sql, params = queries.list_institutions(CONFIG)

    assert "d.institution_sk != @unknown_sk" in sql
    assert "d.is_current" in sql
    assert ("unknown_sk", "unknown") in [(p.name, p.value) for p in params]


def test_list_query_fetches_one_extra_row_to_detect_the_next_page() -> None:
    _, params = queries.list_institutions(CONFIG, limit=50, offset=100)

    values = {p.name: p.value for p in params}
    assert values["limit"] == 51
    assert values["offset"] == 100


def test_filters_are_bound_as_parameters_not_interpolated() -> None:
    sql, params = queries.list_institutions(CONFIG, state="TX'; drop table x --", min_asset=1000)

    assert "drop table" not in sql
    values = {p.name: p.value for p in params}
    assert values["state"] == "TX'; drop table x --"
    assert values["min_asset"] == 1000


def test_financials_query_ranges_on_repdte_and_orders_ascending() -> None:
    sql, params = queries.institution_financials(
        CONFIG, 3510, date_from=datetime.date(2023, 3, 31), date_to=datetime.date(2024, 3, 31)
    )

    assert "order by repdte" in sql
    assert "select *" not in sql
    values = {p.name: p.value for p in params}
    assert values["cert"] == 3510
    assert values["date_from"] == datetime.date(2023, 3, 31)


def test_each_metric_maps_to_a_known_column_in_a_known_mart() -> None:
    for metric in Metric:
        table, column = queries.METRIC_SOURCES[metric]
        sql, _ = queries.rankings(CONFIG, metric)
        assert f"`{CONFIG.table_ref(table)}`" in sql
        assert f"r.{column} as value" in sql


def test_rankings_default_to_the_latest_period_in_the_mart() -> None:
    sql, params = queries.rankings(CONFIG, Metric.roa)

    assert "coalesce(@period, (select max(repdte)" in sql
    assert {p.name: p.value for p in params}["period"] is None


# --- endpoints --------------------------------------------------------------------------


def test_institutions_returns_a_page_and_trims_the_lookahead_row(make_client) -> None:
    rows = [{"cert": i, "name": f"Bank {i}"} for i in range(3)]
    client = make_client(rows=rows)

    body = client.get("/institutions?limit=2").json()

    assert [i["cert"] for i in body["items"]] == [0, 1]
    assert body["has_more"] is True
    assert body["limit"] == 2


def test_institutions_reports_no_more_pages_when_the_lookahead_row_is_absent(make_client) -> None:
    client = make_client(rows=[{"cert": 1, "name": "Bank"}])

    body = client.get("/institutions?limit=2").json()

    assert body["has_more"] is False


def test_state_filter_is_upper_cased_before_it_reaches_the_query(make_client) -> None:
    client = make_client(rows=[])

    client.get("/institutions?state=tx")

    assert _params(client.fake)["state"] == "TX"


def test_unknown_cert_is_a_structured_404(make_client) -> None:
    client = make_client(rows=[])

    response = client.get("/institutions/999999")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "http_404"


def test_invalid_query_parameters_are_a_structured_422(make_client) -> None:
    client = make_client(rows=[])

    response = client.get("/rankings?metric=not_a_metric")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_reversed_date_range_is_rejected_before_querying(make_client) -> None:
    client = make_client(rows=[])

    response = client.get("/institutions/3510/financials?from=2024-01-01&to=2023-01-01")

    assert response.status_code == 422
    assert client.fake.queries == []


def test_financials_series_is_returned_in_report_date_order(make_client) -> None:
    rows = [
        {"cert": 3510, "repdte": datetime.date(2023, 3, 31), "asset": 1.0},
        {"cert": 3510, "repdte": datetime.date(2023, 6, 30), "asset": 2.0},
    ]
    client = make_client(rows=rows)

    body = client.get("/institutions/3510/financials").json()

    assert [i["repdte"] for i in body["items"]] == ["2023-03-31", "2023-06-30"]


def test_rankings_echo_the_requested_metric_on_every_entry(make_client) -> None:
    rows = [{"rank": 1, "cert": 3510, "repdte": datetime.date(2024, 3, 31), "value": 1.5}]
    client = make_client(rows=rows)

    body = client.get("/rankings?metric=roe").json()

    assert body["items"][0]["metric"] == "roe"
    assert body["items"][0]["rank"] == 1


def test_health_reports_ok_from_the_published_freshness_row(make_client) -> None:
    rows = [
        {
            "published_at": datetime.datetime(2026, 8, 6, 12, 0),
            "max_repdte": datetime.date(2025, 12, 31),
            "financials_row_count": 54000,
        }
    ]
    client = make_client(rows=rows)

    body = client.get("/health").json()

    assert body["status"] == "ok"
    assert body["financials_row_count"] == 54000


def test_health_degrades_instead_of_failing_when_the_warehouse_is_unreachable(
    make_client,
) -> None:
    client = make_client(error=NotFound("no such table"))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


def test_warehouse_errors_on_data_endpoints_surface_as_503(make_client) -> None:
    client = make_client(error=NotFound("no such table"))

    response = client.get("/institutions", headers={})

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "warehouse_unavailable"
