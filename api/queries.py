"""SQL builders — pure functions returning `(sql, parameters)`, no client, no I/O.

Two rules hold everywhere in this module:

1. Every user-supplied value is a `ScalarQueryParameter`. Nothing reaches the SQL text by
   string interpolation except table names from `BigQueryConfig` and column names looked up
   in `METRIC_SOURCES` — both closed sets defined here, never user input.
2. Columns are listed explicitly, never `select *`, and filters/ordering go through the
   clustering key: curated tables are clustered on `cert`/`state` rather than partitioned
   on `repdte`, since Sandbox mode's 60-day partition expiration would delete 2023-2025 data
   right after load.
"""

from __future__ import annotations

import datetime

from google.cloud import bigquery

from api.models import Metric
from loaders.config import BigQueryConfig

DIM_INSTITUTION = "dim_institution"
FCT_FINANCIALS = "fct_bank_quarterly_financials"
MART_PROFITABILITY = "mart_profitability_trends"
MART_ASSET_GROWTH = "mart_asset_growth"
MART_LATEST_FINANCIALS = "mart_latest_financials"
FRESHNESS_TABLE = "pipeline_freshness"

# Synthetic dim_institution member for orphan facts to join to; must never appear in responses.
UNKNOWN_INSTITUTION_SK = "unknown"

# Metric -> (mart, column). The mart already computed these; the API only ranks them.
METRIC_SOURCES: dict[Metric, tuple[str, str]] = {
    Metric.roa: (MART_PROFITABILITY, "roa"),
    Metric.roe: (MART_PROFITABILITY, "roe"),
    Metric.roa_qoq_change: (MART_PROFITABILITY, "roa_qoq_change"),
    Metric.roe_qoq_change: (MART_PROFITABILITY, "roe_qoq_change"),
    Metric.asset: (MART_ASSET_GROWTH, "asset"),
    Metric.asset_growth_qoq: (MART_ASSET_GROWTH, "asset_growth_qoq"),
    Metric.asset_growth_yoy: (MART_ASSET_GROWTH, "asset_growth_yoy"),
}

Params = list[bigquery.ScalarQueryParameter]


def _page_params(limit: int, offset: int) -> Params:
    # limit + 1: extra row signals another page exists, no COUNT(*) needed.
    return [
        bigquery.ScalarQueryParameter("limit", "INT64", limit + 1),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]


def list_institutions(
    config: BigQueryConfig,
    *,
    state: str | None = None,
    bkclass: str | None = None,
    min_asset: float | None = None,
    max_asset: float | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[str, Params]:
    # Reads the pre-computed latest-per-cert mart rather than windowing the fact table here:
    # the mart holds one row per bank, so this scans ~4.5k rows instead of 55k per request.
    sql = f"""
        select
            d.cert, d.name, d.city, d.state, d.bkclass, d.regagnt, d.holding_company,
            d.valid_from, f.latest_asset, f.latest_repdte
        from `{config.table_ref(DIM_INSTITUTION)}` d
        left join `{config.table_ref(MART_LATEST_FINANCIALS)}` f on d.cert = f.cert
        where d.is_current
          and d.institution_sk != @unknown_sk
          and (@state is null or d.state = @state)
          and (@bkclass is null or d.bkclass = @bkclass)
          and (@min_asset is null or f.latest_asset >= @min_asset)
          and (@max_asset is null or f.latest_asset <= @max_asset)
        order by d.cert
        limit @limit offset @offset
    """
    params = [
        bigquery.ScalarQueryParameter("unknown_sk", "STRING", UNKNOWN_INSTITUTION_SK),
        bigquery.ScalarQueryParameter("state", "STRING", state),
        bigquery.ScalarQueryParameter("bkclass", "STRING", bkclass),
        bigquery.ScalarQueryParameter("min_asset", "FLOAT64", min_asset),
        bigquery.ScalarQueryParameter("max_asset", "FLOAT64", max_asset),
        *_page_params(limit, offset),
    ]
    return sql, params


def get_institution(config: BigQueryConfig, cert: int) -> tuple[str, Params]:
    sql = f"""
        with latest_financials as (
            select cert, asset, repdte
            from `{config.table_ref(FCT_FINANCIALS)}`
            where cert = @cert
            order by repdte desc
            limit 1
        )
        select
            d.cert, d.name, d.city, d.state, d.bkclass, d.regagnt, d.holding_company,
            d.valid_from, f.asset as latest_asset, f.repdte as latest_repdte
        from `{config.table_ref(DIM_INSTITUTION)}` d
        left join latest_financials f on d.cert = f.cert
        where d.cert = @cert and d.is_current
        limit 1
    """
    return sql, [bigquery.ScalarQueryParameter("cert", "INT64", cert)]


def institution_financials(
    config: BigQueryConfig,
    cert: int,
    *,
    date_from: datetime.date | None = None,
    date_to: datetime.date | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[str, Params]:
    sql = f"""
        select
            cert, repdte, stalp as state, bkclass,
            asset, dep, eq, netinc, roa, roe
        from `{config.table_ref(FCT_FINANCIALS)}`
        where cert = @cert
          and (@date_from is null or repdte >= @date_from)
          and (@date_to is null or repdte <= @date_to)
        order by repdte
        limit @limit offset @offset
    """
    params = [
        bigquery.ScalarQueryParameter("cert", "INT64", cert),
        bigquery.ScalarQueryParameter("date_from", "DATE", date_from),
        bigquery.ScalarQueryParameter("date_to", "DATE", date_to),
        *_page_params(limit, offset),
    ]
    return sql, params


def rankings(
    config: BigQueryConfig,
    metric: Metric,
    *,
    state: str | None = None,
    period: datetime.date | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[str, Params]:
    table, column = METRIC_SOURCES[metric]
    mart = config.table_ref(table)

    # Ranks are computed within the WHERE-filtered set, before paging, so page 2 stays stable.
    sql = f"""
        select
            rank() over (order by r.{column} desc) as rank,
            r.cert,
            i.name,
            r.state,
            r.repdte,
            r.{column} as value
        from `{mart}` r
        left join `{config.table_ref(DIM_INSTITUTION)}` i
            on r.cert = i.cert
            and r.repdte between i.valid_from and i.valid_to
        where r.repdte = coalesce(@period, (select max(repdte) from `{mart}`))
          and (@state is null or r.state = @state)
          and r.{column} is not null
        order by rank
        limit @limit offset @offset
    """
    params = [
        bigquery.ScalarQueryParameter("period", "DATE", period),
        bigquery.ScalarQueryParameter("state", "STRING", state),
        *_page_params(limit, offset),
    ]
    return sql, params


def freshness(config: BigQueryConfig) -> tuple[str, Params]:
    # Pre-computed by the DAG's last task, avoiding a max()/count() scan per health check.
    sql = f"""
        select published_at, max_repdte, financials_row_count
        from `{config.table_ref(FRESHNESS_TABLE)}`
        limit 1
    """
    return sql, []
