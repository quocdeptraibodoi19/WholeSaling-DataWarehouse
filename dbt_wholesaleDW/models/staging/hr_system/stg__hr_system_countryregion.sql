{{ config(materialized='view') }}

select
    countrycode as country_code,
    fullname as country_name,
    extract_date
from {{ source("hr_system", "hr_system_countryregion") }}