{{ config(materialized='view') }}

select
    currencyrateid,
    currencyratedate,
    fromcurrencycode,
    tocurrencycode,
    averagerate,
    endofdayrate,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('wholesale', 'wholesale_system_currencyrate') }}