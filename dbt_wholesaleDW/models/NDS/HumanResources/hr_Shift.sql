{{ config(materialized='view') }}

select 
    shiftid,
    name,
    starttime,
    endtime,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_shift") }}