{{ config(materialized='view') }}

-- Using mapping to map to the BussinessEntityId
with CTE as(
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "employee"
)
select
    CTE.bussinessentityid,
    employeeid as old_employeeid,
    nationalidnumber,
    loginid,
    organizationnode,
    organizationlevel,
    jobtitle,
    birthdate,
    maritalstatus,
    gender,
    hiredate,
    salariedflag,
    vacationhours,
    sickleavehours,
    currentflag,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_employee") }}
inner join CTE
on CTE.external_id = employeeid