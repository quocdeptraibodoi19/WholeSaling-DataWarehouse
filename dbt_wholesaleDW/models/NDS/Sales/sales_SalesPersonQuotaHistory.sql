{{ config(materialized='view') }}

select
    t.bussinessentityid,
    s.quotadate,
    s.salesquota,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from {{ source("hr_system", "hr_system_salespersonquotahistory") }} s
inner join {{ ref("hr_Employee") }} t
on s.employeeid = t.old_employeeid