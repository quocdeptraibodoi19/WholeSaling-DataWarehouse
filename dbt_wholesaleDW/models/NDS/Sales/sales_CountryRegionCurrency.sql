{{ config(materialized='view') }}

select
    countryregioncode,
    currencycode,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("wholesale", "wholesale_system_countryregioncurrency") }}