-- Fails if any cert has two institution_history rows whose validity windows overlap.
select
    a.cert,
    a.valid_from as a_valid_from,
    a.valid_to as a_valid_to,
    b.valid_from as b_valid_from,
    b.valid_to as b_valid_to
from {{ ref('dim_institution') }} a
join {{ ref('dim_institution') }} b
    on a.cert = b.cert
    and a.valid_from < b.valid_to
    and b.valid_from < a.valid_to
    and a.valid_from != b.valid_from
where a.cert is not null
