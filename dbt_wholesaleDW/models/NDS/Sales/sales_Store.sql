{{ config(materialized='view') }}

with CTE as(
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "wholesale_system_store"
)
select distinct 
    CTE.bussinessentityid,
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