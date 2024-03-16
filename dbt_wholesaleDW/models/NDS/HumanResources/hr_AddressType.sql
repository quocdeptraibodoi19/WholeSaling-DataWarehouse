{{ config(materialized='view') }}

select 
    addresstypeid,
    name,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_addresstype") }}