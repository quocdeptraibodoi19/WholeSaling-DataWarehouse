{{ 
    config(
        materialized='incremental',
        unique_key=['bussiness_entity_id', 'updated_at']
    ) 
}}

-- BussinessEntity = Person (customers(online user + store user), Employee) + Vendor + Store

with CTE as (
    select 
        user_id as external_id, 
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("ecom_source") }}_user' as source
    from {{ ref("stg__ecomerce_user") }}
    union all
    select 
        stackholder_id as external_id, 
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("hr_source") }}_stakeholder' as source
    from {{ ref("stg__hr_system_stakeholder") }}
    union all
    select 
        store_id as external_id, 
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("wholesale_source") }}_store' as source
    from {{ ref("stg__wholesale_system_store") }}
    union all
    select 
        employee_id as external_id, 
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("hr_source") }}_employee' as source
    from {{ ref("stg__hr_system_employee") }}
    union all
    select 
        vendor_id as external_id, 
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("product_source") }}_vendor' as source
    from {{ ref("stg__product_management_platform_vendor") }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['external_id', 'source']) }} as bussiness_entity_id,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_deleted,
    is_valid,
    external_id,
    source
from CTE
where 1 = 1
{% if is_incremental() %}

    and updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}

