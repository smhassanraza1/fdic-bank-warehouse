{{
    config(
        materialized='table',
        cluster_by=['cert']
    )
}}

select
    cert,
    repdte,
    stalp as state,
    bkclass,
    roa,
    roe,
    roa - lag(roa) over (partition by cert order by repdte) as roa_qoq_change,
    roe - lag(roe) over (partition by cert order by repdte) as roe_qoq_change
from {{ ref('fct_bank_quarterly_financials') }}
