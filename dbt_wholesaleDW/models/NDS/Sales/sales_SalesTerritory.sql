{{ config(materialized='view') }}

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
        s.extract_date
    from {{ source("ecomerce", "ecomerce_salesterritory") }} s
    inner join {{ ref("countrycode_mapping") }} t
    on s.countryregioncode = t.Ecommerce 
)
select * from mapping_countryregion_cte