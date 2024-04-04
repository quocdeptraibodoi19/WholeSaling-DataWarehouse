{{ config(materialized='table') }}

select 
    t.bussinessentityid,
    s.territoryid,
    s.salesquota,
    s.bonus,
    s.commissionpct,
    s.salesytd,
    s.saleslastyear,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from {{ source("hr_system", "hr_system_salepersons") }} s
inner join {{ ref("hr_Employee") }} t
on s.employeeid = t.old_employeeid