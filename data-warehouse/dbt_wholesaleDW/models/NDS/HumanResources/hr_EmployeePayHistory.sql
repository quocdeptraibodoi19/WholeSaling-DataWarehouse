{{ config(materialized='view') }}

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
    extract_date
from {{ source("hr_system", "hr_system_employeepayhistory") }}
inner join CTE
on CTE.external_id = employeeid