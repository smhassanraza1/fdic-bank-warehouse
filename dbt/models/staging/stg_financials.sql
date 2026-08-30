select
    cert,
    repdte,
    name,
    stalp,
    stname,
    bkclass,
    asset,
    dep,
    eq,
    netinc,
    roa,
    roe,
    timestamp(_ingested_at) as ingested_at,
    _batch_id as batch_id
from {{ source('cleaned', 'cleaned_financials') }}
