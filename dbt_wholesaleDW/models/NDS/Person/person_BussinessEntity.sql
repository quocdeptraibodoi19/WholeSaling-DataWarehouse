{{ config(materialized='incremental') }}

-- BussinessEntity = Person (customers(online user + store user), Employee) + Vendor + Store

with CTE as (
    select 
        userid as external_id, 
        modifieddate, 
        is_deleted, 
        extract_date,
        "ecom_user" as source
    from {{ source("ecomerce", "ecomerce_user") }}
    union all
    select 
        stackholderid as external_id, 
        modifieddate, 
        is_deleted, 
        extract_date,
        "stakeholder" as source
    from {{ source("hr_system", "hr_system_stakeholder") }}
    union all
    select 
        storeid as external_id, 
        modifieddate, 
        is_deleted, 
        extract_date,
        "store" as source
    from {{ source("wholesale", "wholesale_system_store") }}
    union all
    select 
        employeeid as external_id, 
        modifieddate, 
        is_deleted, 
        extract_date,
        "employee" as source
    from {{ source("hr_system", "hr_system_employee") }}
    union all
    select 
        vendorid as external_id, 
        modifieddate, 
        is_deleted, 
        extract_date,
        "vendor" as source
    from {{ source("production", "product_management_platform_vendor") }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['external_id', 'source']) }} as bussinessentityid,
    modifieddate, 
    is_deleted, 
    extract_date,
    external_id,
    source
from CTE

{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}

