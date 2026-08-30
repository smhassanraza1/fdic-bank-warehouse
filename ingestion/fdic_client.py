"""Client for the FDIC BankFind Suite API.

Handles offset pagination (capped at the API's 10k `limit`) and retry/backoff on transient
failures. The HTTP transport and the sleep function are both injectable so unit tests can
exercise pagination and retry logic without any network access or real waiting.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 10_000

# 4xx means our request is wrong, so only server-side codes are retried.
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class FdicApiError(Exception):
    """Raised when a request to the FDIC API fails after all retries are exhausted."""


class FdicClient:
    def __init__(
        self,
        base_url: str,
        *,
        transport: httpx.BaseTransport | None = None,
        max_retries: int = 5,
        backoff_base_seconds: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self._client = httpx.Client(
            base_url=self.base_url,
            transport=transport,
            timeout=timeout_seconds,
            follow_redirects=True,
        )
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._sleep = sleep

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> FdicClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def iter_records(
        self,
        endpoint: str,
        *,
        filters: str | None = None,
        fields: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        page_size: int = MAX_PAGE_SIZE,
    ) -> Iterator[dict[str, Any]]:
        """Yield every record for `endpoint`, paginating on `meta.total` until exhausted.

        Each yielded item is the API response item verbatim (`{"data": {...}, "score": ...}`) —
        unwrapping to just the record is a cleaning-layer concern, not a raw-ingestion one.

        Deep offset pagination against Elasticsearch is only stable if the result order is
        deterministic — without an explicit `sort_by` on a key unique within the filtered result
        set, ties can be broken differently between requests and rows get silently dropped or
        duplicated across the offset boundary. `get_institutions`/`get_financials`/`get_sod`
        default `sort_by` accordingly; pass your own only if you know it's unique for your filter.
        """
        if page_size > MAX_PAGE_SIZE:
            raise ValueError(f"page_size {page_size} exceeds FDIC API max of {MAX_PAGE_SIZE}")

        offset = 0
        total: int | None = None
        while total is None or offset < total:
            params: dict[str, Any] = {"limit": page_size, "offset": offset, "format": "json"}
            if filters:
                params["filters"] = filters
            if fields:
                params["fields"] = fields
            if sort_by:
                params["sort_by"] = sort_by
            if sort_order:
                params["sort_order"] = sort_order

            body = self._get(endpoint, params)
            total = body.get("meta", {}).get("total", 0)
            page = body.get("data", [])
            if not page:
                if offset < total:
                    logger.warning(
                        "%s: empty page at offset=%d but meta.total=%d — stopping early",
                        endpoint,
                        offset,
                        total,
                    )
                break

            yield from page
            offset += len(page)

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.get(endpoint, params=params)
            except httpx.TransportError as exc:
                last_error = exc
            else:
                if response.status_code == 200:
                    return response.json()
                if response.status_code not in _RETRYABLE_STATUS_CODES:
                    raise FdicApiError(
                        f"GET {endpoint} failed with {response.status_code}: {response.text[:500]}"
                    )
                last_error = FdicApiError(f"GET {endpoint} got retryable {response.status_code}")

            if attempt < self._max_retries:
                self._sleep(self._backoff_base_seconds * (2**attempt))

        raise FdicApiError(
            f"GET {endpoint} failed after {self._max_retries + 1} attempts"
        ) from last_error

    def get_institutions(self, **kwargs: Any) -> Iterator[dict[str, Any]]:
        kwargs.setdefault("sort_by", "CERT")
        kwargs.setdefault("sort_order", "ASC")
        return self.iter_records("institutions", **kwargs)

    def get_financials(self, **kwargs: Any) -> Iterator[dict[str, Any]]:
        kwargs.setdefault("sort_by", "CERT")
        kwargs.setdefault("sort_order", "ASC")
        return self.iter_records("financials", **kwargs)

    def get_sod(self, **kwargs: Any) -> Iterator[dict[str, Any]]:
        # UNINUMBR is unique per branch, giving a stable single-field sort key.
        kwargs.setdefault("sort_by", "UNINUMBR")
        kwargs.setdefault("sort_order", "ASC")
        return self.iter_records("sod", **kwargs)
