{{ 
    config(
        materialized='incremental',
        unique_key=['addressid', 'updated_at'],
        file_format='delta',
        incremental_strategy='merge'
    ) 
}}

with CTE as (
    select
        vendor_address_id as id,
        old_addressid,
        address_type_id,
        addressline1,
        addressline2,
        state_province_id,
        city,
        postal_code,
        spatial_location,
        '{{ env_var("product_source") }}_vendor' as source,
        vendor_id as source_key,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__product_management_platform_vendoraddress") }}
    union all
    select
        user_address_id as id,
        old_addressid,
        address_type_id,
        addressline1,
        addressline2,
        state_province_id,
        city,
        postal_code,
        spatial_location,
        '{{ env_var("ecom_source") }}_user' as source,
        user_id as source_key,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__ecomerce_useraddress") }}
    union all
    select
        employee_address_id as id,
        old_addressid,
        address_type_id,
        addressline1,
        addressline2,
        state_province_id,
        city,
        postal_code,
        spatial_location,
        '{{ env_var("hr_source") }}_employee' as source,
        employee_id as source_key,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__hr_system_employeeaddress") }}
    union all
    select
        store_address_id as id,
        old_addressid,
        address_type_id,
        addressline1,
        addressline2,
        state_province_id,
        city,
        postal_code,
        spatial_location,
        '{{ env_var("wholesale_source") }}_store' as source,
        store_id as source_key,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__wholesale_system_storeaddress") }}
)
select
    {{ dbt_utils.generate_surrogate_key(['id', 'source']) }} as addressid,
    old_addressid,
    address_type_id,
    addressline1,
    addressline2,
    state_province_id,
    city,
    postal_code,
    spatial_location,
    source,
    source_key,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_deleted,
    is_valid
from CTE
where 1 = 1
{% if is_incremental() %}

    and updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}