select
    cert,
    uninumbr,
    namefull,
    citybr,
    stalpbr,
    stnamebr,
    depsumbr,
    asset,
    year,
    repdte,
    timestamp(_ingested_at) as ingested_at,
    _batch_id as batch_id
from {{ source('cleaned', 'cleaned_branch_deposits') }}
