{{ config(materialized='view') }}

-- Using mapping to map to the BussinessEntityId
select
    employeeid,
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