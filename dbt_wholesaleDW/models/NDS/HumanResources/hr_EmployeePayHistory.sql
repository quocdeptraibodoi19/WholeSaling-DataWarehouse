{{ config(materialized='table') }}

with CTE as(
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "employee"
)
select
    CTE.bussinessentityid,
    ratechangedate,
    rate,
    payfrequency,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_employeepayhistory") }}
inner join CTE
on CTE.external_id = employeeid