{{ config(materialized='table') }}

with cte as(
    select
        t.Wholesaling as countryregioncode,
        t.HR as hr_countryregioncode,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    {{ source("wholesale", "wholesale_system_countryregion") }} s
    inner join {{ ref("countrycode_mapping") }} t
    on s.countryregioncode = t.Wholesaling
),
hr_name_cte as (
    select
        s.countryregioncode,
        t.fullname as `name`,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from cte s
    inner join {{ source("hr_system", "hr_system_countryregion") }} t
    on s.hr_countryregioncode = t.countrycode
)
select * from hr_name_cte