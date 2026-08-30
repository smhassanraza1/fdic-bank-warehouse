{{
    config(
        materialized='table',
        cluster_by=['cert']
    )
}}

with history as (
    select
        to_hex(md5(concat(cast(cert as string), '|', cast(valid_from as string)))) as institution_sk,
        cert,
        name,
        city,
        state,
        bkclass,
        regagnt,
        holding_company,
        valid_from,
        valid_to,
        is_current
    from {{ ref('stg_institution_history') }}
),

-- Some certs appear in cleaned_financials but were never returned by the /institutions
-- endpoint (closed/merged banks outside its scope). Facts for those certs still need a
-- row to join to, so every fact gets a surrogate key rather than being silently dropped.
unknown_member as (
    select
        'unknown' as institution_sk,
        cast(null as int64) as cert,
        'Unknown Institution' as name,
        cast(null as string) as city,
        cast(null as string) as state,
        cast(null as string) as bkclass,
        cast(null as string) as regagnt,
        cast(null as string) as holding_company,
        cast('1900-01-01' as date) as valid_from,
        cast('9999-12-31' as date) as valid_to,
        true as is_current
)

select * from history
union all
select * from unknown_member
