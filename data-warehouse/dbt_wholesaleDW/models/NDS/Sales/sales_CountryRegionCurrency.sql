{{ config(materialized='view') }}

select
    countryregioncode,
    currencycode,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("wholesale", "wholesale_system_countryregioncurrency") }}