{{ config(materialized='table') }}

with mapping_country_region as (
    select

        t.country_name,
        s.sale_region,
        s.sale_region_code,
        s.HR as {{ env_var("hr_source") }},
        s.Wholesaling as {{ env_var("wholesale_source") }},
        s.Ecommerce as {{ env_var("ecom_source") }}

    from {{ ref("countrycode_mapping") }} s
    inner join {{ ref("stg__hr_system_countryregion") }} t
        on s.HR = t.country_code
)
select * from mapping_country_region