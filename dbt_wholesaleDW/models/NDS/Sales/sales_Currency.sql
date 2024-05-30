{{ config(materialized='view') }}

select
    currencycode,
    `name`,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('wholesale', 'wholesale_system_currency') }}
