{{ config(materialized='view') }}

select 
    phonenumbertypeid,
    name,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_phonenumbertype") }}