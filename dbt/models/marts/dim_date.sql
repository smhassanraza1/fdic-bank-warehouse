{{
    config(
        materialized='table'
    )
}}

select distinct
    repdte as date_day,
    extract(year from repdte) as year,
    extract(quarter from repdte) as quarter,
    concat(cast(extract(year from repdte) as string), '-Q', cast(extract(quarter from repdte) as string)) as year_quarter
from {{ ref('stg_financials') }}
