{{
    config(
        materialized='table',
        cluster_by=['cert']
    )
}}

-- BigQuery Sandbox has no billing account, so MERGE/DML incremental strategies are
-- rejected outright ("DML queries are not allowed in the free tier"). The upstream
-- source is a full WRITE_TRUNCATE snapshot each run anyway, so a full-table rebuild
-- here is both the only option and already idempotent -- the same fallback every
-- upstream layer takes for this constraint.
select
    coalesce(d.institution_sk, 'unknown') as institution_sk,
    f.cert,
    f.repdte,
    f.stalp,
    f.stname,
    f.bkclass,
    f.asset,
    f.dep,
    f.eq,
    f.netinc,
    f.roa,
    f.roe,
    f.ingested_at,
    f.batch_id
from {{ ref('stg_financials') }} f
left join {{ ref('dim_institution') }} d
    on f.cert = d.cert
    and f.repdte between d.valid_from and d.valid_to
