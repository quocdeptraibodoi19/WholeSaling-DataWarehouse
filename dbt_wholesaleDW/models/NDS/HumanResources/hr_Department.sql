{{ config(materialized='view') }}

select
    departmentid,
    name,
    groupname,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("hr_system", "hr_system_department") }}