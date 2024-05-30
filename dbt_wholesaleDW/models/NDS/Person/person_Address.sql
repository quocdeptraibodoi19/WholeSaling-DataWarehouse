{{ config(materialized='table') }}

with CTE as (
    select
        vendor_address_id as id,
        old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        '{{ env_var("product_source") }}' as source,
        vendorid as source_key,
        extract_date,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__product_management_platform_vendoraddress") }}
    union all
    select
        user_address_id as id,
        old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        '{{ env_var("ecom_source") }}' as source,
        userid as source_key,
        extract_date,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__ecomerce_useraddress") }}
    union all
    select
        employee_address_id as id,
        old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        '{{ env_var("hr_source") }}' as source,
        employeeid as source_key,
        extract_date,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__hr_system_employeeaddress") }}
    union all
    select
        store_address_id as id,
        old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        '{{ env_var("wholesale_source") }}' as source,
        storeid as source_key,
        extract_date,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("stg__wholesale_system_storeaddress") }}
)
select
    {{ dbt_utils.generate_surrogate_key(['id', 'source']) }} as addressid,
    old_addressid,
    addresstypeid,
    addressline1,
    addressline2,
    stateprovinceid,
    postalcode,
    spatiallocation,
    modifieddate,
    source,
    source_key,
    extract_date,
    valid_from,
    valid_to,
    is_deleted,
    is_valid
from CTE
