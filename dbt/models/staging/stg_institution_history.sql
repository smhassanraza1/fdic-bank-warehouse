select
    cert,
    name,
    city,
    state,
    bkclass,
    regagnt,
    holding_company,
    valid_from,
    valid_to,
    is_current,
    _hash
from {{ source('cleaned', 'cleaned_institution_history') }}
