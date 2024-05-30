{{ config(materialized='table') }}

with CTE as (
    select
        addressid as old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        "vendor" as source,
        vendorid as source_key
    from {{ source("production", "product_management_platform_vendoraddress") }}
    union all
    select 
        addressid as old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        "ecom_user" as source,
        userid as source_key
    from {{ source("ecomerce", "ecomerce_useraddress") }}
    union all
    select 
        addressid as old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        "employee" as source,
        employeeid as source_key
    from {{ source("hr_system", "hr_system_employeeaddress") }}
    union all
    select 
        addressid as old_addressid,
        addresstypeid,
        addressline1,
        addressline2,
        stateprovinceid,
        postalcode,
        spatiallocation,
        modifieddate,
        "store" as source,
        storeid as source_key
    from {{ source("wholesale", "wholesale_system_storeaddress") }}
)
select
    {{ dbt_utils.generate_surrogate_key(['old_addressid', 'source']) }} as addressid,
    old_addressid,
    addresstypeid,
    addressline1,
    addressline2,
    stateprovinceid,
    postalcode,
    spatiallocation,
    modifieddate,
    source,
    source_key
from CTE
