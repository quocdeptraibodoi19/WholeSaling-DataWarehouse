{{ config(materialized='view') }}

select
    countryregioncode,
    currencycode,
    modifieddate,
    is_deleted,
    date_partition
from {{"wholesale", "wholesale_system_countryregioncurrency"}}