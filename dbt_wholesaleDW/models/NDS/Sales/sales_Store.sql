{{ config(materialized='view') }}

with CTE as(
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "store"
)
select distinct 
    CTE.bussinessentityid,
    storeid as old_storeid,
    name,
    employeenationalidnumber as salespersonid,
    demographics,
    modifieddate,
    is_deleted,
    date_partition
from 
{{ source('wholesale', 'wholesale_system_store') }}
inner join CTE
on CTE.external_id = storeid