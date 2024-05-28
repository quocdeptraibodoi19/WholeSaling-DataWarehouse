{{ config(materialized='view') }}

select 
    addresstypeid,
    name,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("hr_system", "hr_system_addresstype") }}