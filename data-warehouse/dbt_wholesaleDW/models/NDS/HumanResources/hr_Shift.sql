{{ config(materialized='view') }}

select 
    shiftid,
    name,
    starttime,
    endtime,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("hr_system", "hr_system_shift") }}