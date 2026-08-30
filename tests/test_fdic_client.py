"""Network-free tests for FdicClient: pagination and retry/backoff."""

from __future__ import annotations

import httpx
import pytest

from ingestion.fdic_client import MAX_PAGE_SIZE, FdicApiError, FdicClient


def _page_response(total: int, offset: int, limit: int) -> dict:
    remaining = max(total - offset, 0)
    n = min(limit, remaining)
    data = [{"data": {"CERT": offset + i}, "score": 0} for i in range(n)]
    return {"meta": {"total": total}, "data": data}


def test_pagination_collects_all_pages_across_offsets() -> None:
    total = 25
    requests_seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        limit = int(request.url.params["limit"])
        requests_seen.append(offset)
        return httpx.Response(200, json=_page_response(total, offset, limit))

    client = FdicClient(
        "https://example.test/api/", transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    records = list(client.iter_records("institutions", page_size=10))

    assert [r["data"]["CERT"] for r in records] == list(range(total))
    assert requests_seen == [0, 10, 20]


def test_pagination_stops_on_empty_page_even_if_total_says_more() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        if offset == 0:
            body = {"meta": {"total": 100}, "data": [{"data": {"CERT": 1}}]}
        else:
            body = {"meta": {"total": 100}, "data": []}
        return httpx.Response(200, json=body)

    client = FdicClient(
        "https://example.test/api/", transport=httpx.MockTransport(handler), sleep=lambda _: None
    )
    records = list(client.iter_records("institutions", page_size=10))

    assert len(records) == 1


def test_page_size_over_api_max_rejected() -> None:
    client = FdicClient("https://example.test/api/", transport=httpx.MockTransport(lambda r: None))
    with pytest.raises(ValueError):
        list(client.iter_records("institutions", page_size=MAX_PAGE_SIZE + 1))


def test_retries_transient_5xx_then_succeeds() -> None:
    attempts = {"n": 0}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"meta": {"total": 1}, "data": [{"data": {"CERT": 1}}]})

    client = FdicClient(
        "https://example.test/api/",
        transport=httpx.MockTransport(handler),
        max_retries=5,
        backoff_base_seconds=0.01,
        sleep=sleeps.append,
    )
    records = list(client.iter_records("institutions"))

    assert len(records) == 1
    assert attempts["n"] == 3
    assert sleeps == [0.01, 0.02]


def test_non_retryable_4xx_raises_immediately() -> None:
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(400, text="bad filter syntax")

    client = FdicClient(
        "https://example.test/api/",
        transport=httpx.MockTransport(handler),
        sleep=lambda _: pytest.fail("should not sleep on a non-retryable error"),
    )
    with pytest.raises(FdicApiError, match="400"):
        list(client.iter_records("institutions"))

    assert attempts["n"] == 1


def test_exhausting_retries_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = FdicClient(
        "https://example.test/api/",
        transport=httpx.MockTransport(handler),
        max_retries=2,
        backoff_base_seconds=0.0,
        sleep=lambda _: None,
    )
    with pytest.raises(FdicApiError):
        list(client.iter_records("institutions"))
