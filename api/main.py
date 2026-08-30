"""FastAPI serving layer over the curated BigQuery models — the last arrow in the pipeline.

Read-only. Every response is an explicit Pydantic model, every list endpoint is paginated, and
every error is the same `{"error": {...}}` envelope so a client parses failures the same way
whether they came from FastAPI's validation or from BigQuery.

Run with:

    uv run --group api uvicorn api.main:app --reload

Reads BigQuery credentials from the environment (see `.env.example`).
"""

from __future__ import annotations

import datetime
import logging
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery

from api import queries
from api.deps import get_client, get_config
from api.models import (
    ErrorResponse,
    FinancialPeriod,
    Health,
    HealthStatus,
    Institution,
    Metric,
    Page,
    RankingEntry,
)
from loaders.config import BigQueryConfig

logger = logging.getLogger(__name__)

app = FastAPI(
    title="FIG Data Platform API",
    description="Read-only access to FDIC bank financials in the curated BigQuery layer.",
    version="1.0.0",
    responses={
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)

ClientDep = Annotated[bigquery.Client, Depends(get_client)]
ConfigDep = Annotated[BigQueryConfig, Depends(get_config)]

LimitQuery = Annotated[int, Query(ge=1, le=500)]
OffsetQuery = Annotated[int, Query(ge=0)]
StateQuery = Annotated[str | None, Query(min_length=2, max_length=2, pattern="^[A-Za-z]{2}$")]


def _error(status: int, code: str, message: str, detail: Any = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code, "message": message, "detail": detail}},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return _error(exc.status_code, code=f"http_{exc.status_code}", message=str(exc.detail))


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return _error(
        422,
        code="invalid_request",
        message="Request parameters failed validation.",
        detail=exc.errors(),
    )


@app.exception_handler(GoogleAPIError)
async def bigquery_exception_handler(request: Request, exc: GoogleAPIError) -> JSONResponse:
    # Upstream unavailability, not a client error -- 503 means retry, not fix your request.
    logger.exception("BigQuery query failed")
    return _error(503, code="warehouse_unavailable", message="The warehouse is not available.")


def _run(client: bigquery.Client, sql: str, params: queries.Params) -> list[dict[str, Any]]:
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]


def _paginate(rows: list[dict[str, Any]], limit: int, offset: int, model: type) -> Page:
    """Split the `limit + 1` rows the query asked for into a page plus a `has_more` flag."""
    has_more = len(rows) > limit
    items = [model(**row) for row in rows[:limit]]
    return Page(items=items, limit=limit, offset=offset, has_more=has_more)


@app.get("/institutions", response_model=Page[Institution])
def list_institutions(
    client: ClientDep,
    config: ConfigDep,
    state: StateQuery = None,
    bkclass: Annotated[str | None, Query(max_length=4)] = None,
    min_asset: Annotated[float | None, Query(ge=0)] = None,
    max_asset: Annotated[float | None, Query(ge=0)] = None,
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> Page[Institution]:
    sql, params = queries.list_institutions(
        config,
        state=state.upper() if state else None,
        bkclass=bkclass.upper() if bkclass else None,
        min_asset=min_asset,
        max_asset=max_asset,
        limit=limit,
        offset=offset,
    )
    return _paginate(_run(client, sql, params), limit, offset, Institution)


@app.get("/institutions/{cert}", response_model=Institution)
def get_institution(
    client: ClientDep,
    config: ConfigDep,
    cert: Annotated[int, Path(ge=1)],
) -> Institution:
    sql, params = queries.get_institution(config, cert)
    rows = _run(client, sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No current institution with cert {cert}.")
    return Institution(**rows[0])


@app.get("/institutions/{cert}/financials", response_model=Page[FinancialPeriod])
def institution_financials(
    client: ClientDep,
    config: ConfigDep,
    cert: Annotated[int, Path(ge=1)],
    date_from: Annotated[datetime.date | None, Query(alias="from")] = None,
    date_to: Annotated[datetime.date | None, Query(alias="to")] = None,
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> Page[FinancialPeriod]:
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="`from` must not be later than `to`.")
    sql, params = queries.institution_financials(
        config, cert, date_from=date_from, date_to=date_to, limit=limit, offset=offset
    )
    return _paginate(_run(client, sql, params), limit, offset, FinancialPeriod)


@app.get("/rankings", response_model=Page[RankingEntry])
def rankings(
    client: ClientDep,
    config: ConfigDep,
    metric: Metric = Metric.roa,
    state: StateQuery = None,
    period: datetime.date | None = None,
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> Page[RankingEntry]:
    sql, params = queries.rankings(
        config,
        metric,
        state=state.upper() if state else None,
        period=period,
        limit=limit,
        offset=offset,
    )
    rows = [row | {"metric": metric} for row in _run(client, sql, params)]
    return _paginate(rows, limit, offset, RankingEntry)


@app.get("/health", response_model=Health)
def health(client: ClientDep, config: ConfigDep) -> Health:
    """Liveness plus warehouse freshness.

    Always 200 while the process is serving: an unreachable or not-yet-published warehouse is
    reported as `degraded` in the body, so a restart loop isn't triggered for what is really a
    pipeline problem.
    """
    sql, params = queries.freshness(config)
    try:
        rows = _run(client, sql, params)
    except NotFound:
        return Health(status=HealthStatus.degraded, detail="Freshness table does not exist yet.")
    except GoogleAPIError as exc:
        logger.warning("freshness lookup failed: %s", exc)
        return Health(status=HealthStatus.degraded, detail="Warehouse is unreachable.")

    if not rows:
        return Health(status=HealthStatus.degraded, detail="No freshness metric published yet.")
    return Health(status=HealthStatus.ok, **rows[0])
