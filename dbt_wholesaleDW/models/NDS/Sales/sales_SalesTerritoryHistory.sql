{{ config(materialized='table') }}

with employee_mapping_cte as (
    select
        t.bussinessentityid,
        s.territoryid,
        s.startdate,
        s.enddate,
        s.modifieddate
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_saleterritoryrephistory") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.employeeid = t.external_id and t.source = "employee"
)
select * from employee_mapping_cte