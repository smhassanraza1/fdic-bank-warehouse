{{
    config(
        materialized='table',
        cluster_by=['state']
    )
}}

with bank_state_deposits as (
    select
        cert,
        stalpbr as state,
        repdte,
        sum(depsumbr) as total_deposits
    from {{ ref('stg_branch_deposits') }}
    group by cert, stalpbr, repdte
),

state_totals as (
    select
        state,
        repdte,
        sum(total_deposits) as state_total_deposits
    from bank_state_deposits
    group by state, repdte
),

shares as (
    select
        b.cert,
        b.state,
        b.repdte,
        b.total_deposits,
        safe_divide(b.total_deposits, t.state_total_deposits) as deposit_share
    from bank_state_deposits b
    join state_totals t
        on b.state = t.state and b.repdte = t.repdte
)

select
    cert,
    state,
    repdte,
    total_deposits,
    deposit_share,
    sum(power(deposit_share, 2)) over (partition by state, repdte) * 10000 as state_hhi
from shares
