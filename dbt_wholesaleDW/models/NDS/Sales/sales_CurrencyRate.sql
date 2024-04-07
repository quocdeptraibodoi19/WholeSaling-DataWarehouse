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
    date_partition
from {{ source('wholesale', 'wholesale_system_currencyrate') }}