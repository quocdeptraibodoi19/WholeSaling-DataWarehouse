{{ config(materialized='view') }}

select 
    employeeid,
    territoryid,
    salesquota,
    bonus,
    commissionpct,
    salesytd,
    saleslastyear,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_salepersons") }}