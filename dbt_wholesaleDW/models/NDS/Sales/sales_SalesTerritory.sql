{{ config(materialized='table') }}

with mapping_countryregion_cte as (
    select
        s.territoryid,
        s.`name`,
        t.Wholesaling as countryregioncode,
        s.`group`,
        s.salesytd,
        s.saleslastyear,
        s.costlastyear,
        s.costytd,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_salesterritory") }} s
    inner join {{ ref("countrycode_mapping") }} t
    on s.countryregioncode = t.HR 
)
select * from mapping_countryregion_cte