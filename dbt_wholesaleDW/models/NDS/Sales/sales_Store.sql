{{ config(materialized='view') }}

select distinct 
    storeid,
    name,
    employeenationalidnumber as salespersonid,
    demographics,
    modifieddate,
    is_deleted,
    date_partition
from {{ source('wholesale', 'wholesale_system_store') }}