{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['person_Address.addressid']) }} as address_key,
    person_Address.addressid as address_id,
    person_Address.city as city_name,
    person_Address.postal_code, 
    person_Address.addressline1 || ' '|| coalesce(person_Address.addressline2, '') as address_line,
    person_StateProvince.state_province_name as state_name,
    person_CountryRegion.country_name,
    case
        when person_Address.is_valid = 0 or person_StateProvince.is_valid = 0 then 0
        else 1
    end as is_valid
from {{ ref('person_Address') }}
left join {{ ref('person_StateProvince') }}
    on person_Address.state_province_id = person_StateProvince.state_province_id
left join {{ ref('person_CountryRegion') }} 
    on person_StateProvince.country_region_code = person_CountryRegion.{{ env_var("wholesale_source") }}