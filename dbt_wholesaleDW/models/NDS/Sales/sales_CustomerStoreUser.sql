{{ config(materialized='view') }}

with CTE as(
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "wholesale_system_storecustomer"
),
CTE2 as (
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "wholesale_system_store_storerep"
)
select 
    CTE2.bussinessentityid as storerepid,
    s.storerepid as old_storerepid,
    CTE.bussinessentityid as storeid,
    s.storeid as old_storeid,
    s.accountnumber,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from {{ source("wholesale", "wholesale_system_storecustomer") }} s
inner join CTE
on CTE.external_id = s.storeid
left join CTE2
on CTE2.external_id = s.storerepid