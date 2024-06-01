{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['sales_Customer.customer_id']) }} as customer_key,
    sales_Customer.customer_id,
    person_Person.bussiness_entity_id as person_business_entity_id,
    person_Person.title,
    person_Person.first_name || ' '|| person_Person.last_name || ' '|| person_Person.middle_name as full_name,
    person_Person.house_owner_flag, 
    person_Person.occupation, 
    person_Person.marital_status, 
    person_Person.commuted_distance, 
    person_Person.education, 
    person_Person.gender,
    person_Person.number_cars_owned, 
    person_Person.total_children, 
    person_Person.birthdate, 
    person_Person.date_first_purchase,
    person_CountryRegion.country_name as country,
    person_Address.city,
    person_AddressType.address_type_name as address_type,
    person_StateProvince.state_province_name as state,
    person_Address.postal_code,
    person_Address.addressline1,
    person_Address.addressline2,
    case
        when sales_Customer.is_valid = 0 
            or person_Person.is_valid = 0
            or person_BusinessEntityAddress.is_valid = 0
            or person_Address.is_valid = 0 
            or person_StateProvince.is_valid = 0
            or person_AddressType.is_valid = 0
            then 0
        else 1
    end as is_valid
from {{ ref('sales_Customer') }}
left join {{ ref('person_Person') }} 
    on sales_Customer.person_id = person_Person.bussiness_entity_id
left join {{ ref('person_BusinessEntityAddress') }}
    on person_BusinessEntityAddress.bussiness_entity_id = person_Person.bussiness_entity_id
left join {{ ref('person_Address') }} 
    on person_Address.addressid = person_BusinessEntityAddress.address_id
left join {{ ref('person_AddressType') }}
    on person_Address.address_type_id = person_AddressType.address_type_id
left join {{ ref('person_StateProvince') }} 
    on person_StateProvince.state_province_id = person_Address.state_province_id
left join {{ ref('person_CountryRegion') }} 
    on person_CountryRegion.{{ env_var("wholesale_source") }} = person_StateProvince.country_region_code
