"""Response models — the API's public contract.

Every endpoint declares an explicit model rather than returning raw BigQuery rows, so a
column rename in the curated layer surfaces as a validation error here instead of silently
changing the shape consumers depend on.
"""

from __future__ import annotations

import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Page[T](BaseModel):
    """One envelope for every list endpoint.

    `has_more` instead of a total count: a total needs a second full scan of the table on
    every request, and BigQuery Sandbox bills 1 TB/month of scanned bytes. Pages are fetched
    with `limit + 1` rows, so the flag costs nothing.
    """

    items: list[T]
    limit: int
    offset: int
    has_more: bool


class Institution(BaseModel):
    cert: int
    name: str | None = None
    city: str | None = None
    state: str | None = None
    bkclass: str | None = None
    regagnt: str | None = None
    holding_company: str | None = None
    valid_from: datetime.date | None = None
    latest_asset: float | None = Field(
        default=None,
        description="Total assets ($k) as of `latest_repdte` — the most recent quarter this "
        "bank filed, which for a bank that stopped filing is older than the warehouse's.",
    )
    latest_repdte: datetime.date | None = None


class FinancialPeriod(BaseModel):
    cert: int
    repdte: datetime.date
    state: str | None = None
    bkclass: str | None = None
    asset: float | None = None
    dep: float | None = None
    eq: float | None = None
    netinc: float | None = None
    roa: float | None = None
    roe: float | None = None


class Metric(StrEnum):
    """Rankable metrics.

    A closed enum, not free text: the metric selects a column name, and column names cannot be
    query parameters. Everything outside this set is rejected by Pydantic before reaching SQL.
    """

    roa = "roa"
    roe = "roe"
    roa_qoq_change = "roa_qoq_change"
    roe_qoq_change = "roe_qoq_change"
    asset = "asset"
    asset_growth_qoq = "asset_growth_qoq"
    asset_growth_yoy = "asset_growth_yoy"


class RankingEntry(BaseModel):
    rank: int
    cert: int
    name: str | None = None
    state: str | None = None
    repdte: datetime.date
    metric: Metric
    value: float | None = None


class HealthStatus(StrEnum):
    ok = "ok"
    degraded = "degraded"


class Health(BaseModel):
    status: HealthStatus
    published_at: datetime.datetime | None = None
    max_repdte: datetime.date | None = None
    financials_row_count: int | None = None
    detail: str | None = None


class ErrorBody(BaseModel):
    code: str
    message: str
    detail: object | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
