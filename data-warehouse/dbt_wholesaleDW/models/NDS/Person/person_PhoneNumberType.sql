{{ config(materialized='view') }}

select 
    phonenumbertypeid,
    name,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("hr_system", "hr_system_phonenumbertype") }}