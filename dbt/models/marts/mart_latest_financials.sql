{{
    config(
        materialized='table',
        cluster_by=['cert']
    )
}}

-- One row per bank: its most recently *reported* quarter, which is not the same as the table's
-- most recent quarter -- a lapsed filer keeps its last known figures rather than dropping out.
--
-- This exists so the serving layer doesn't have to window the whole fact table on every
-- /institutions request. Computing it once per DAG run and reading ~4.5k rows beats recomputing
-- it across 55k rows per request, for the same reason /health reads a pre-computed freshness
-- row instead of aggregating the fact table.
select
    cert,
    repdte as latest_repdte,
    asset as latest_asset
from (
    select
        cert,
        repdte,
        asset,
        row_number() over (partition by cert order by repdte desc) as rn
    from {{ ref('fct_bank_quarterly_financials') }}
)
where rn = 1
