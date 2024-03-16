{{ config(materialized='view') }}

select
    departmentid,
    name,
    groupname,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_department") }}