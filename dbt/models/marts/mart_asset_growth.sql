{{
    config(
        materialized='table',
        cluster_by=['cert']
    )
}}

with growth as (
    select
        cert,
        repdte,
        stalp as state,
        bkclass,
        asset,
        lag(asset) over (partition by cert order by repdte) as asset_prior_quarter,
        lag(asset, 4) over (partition by cert order by repdte) as asset_prior_year
    from {{ ref('fct_bank_quarterly_financials') }}
)

select
    cert,
    repdte,
    state,
    bkclass,
    asset,
    safe_divide(asset - asset_prior_quarter, asset_prior_quarter) as asset_growth_qoq,
    safe_divide(asset - asset_prior_year, asset_prior_year) as asset_growth_yoy,
    rank() over (
        partition by repdte
        order by safe_divide(asset - asset_prior_quarter, asset_prior_quarter) desc
    ) as qoq_growth_rank
from growth
