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
    departmentid,
    shiftid,
    startdate,
    enddate,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_employeedepartmenthistory") }}
inner join CTE
on CTE.external_id = employeeid