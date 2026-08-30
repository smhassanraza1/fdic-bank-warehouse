-- Fails if fct_bank_quarterly_financials has more than one row per (cert, repdte).
select cert, repdte, count(*) as row_count
from {{ ref('fct_bank_quarterly_financials') }}
group by cert, repdte
having count(*) > 1
