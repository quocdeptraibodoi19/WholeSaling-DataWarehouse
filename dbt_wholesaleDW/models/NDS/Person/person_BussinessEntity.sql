{{ config(materialized='incremental') }}

-- BussinessEntity = Person (customers(online user + store user), Employee) + Vendor + Store

with CTE as (
    select 
        userid as external_id, 
        modifieddate, 
        is_deleted, 
        date_partition,
        "ecomerce_user" as table_source
    from {{ source("ecomerce", "ecomerce_user") }}
    union all
    select 
        customerid as external_id, 
        modifieddate, 
        is_deleted, 
        date_partition,
        "wholesale_system_storecustomer" as table_source
    from {{ source("wholesale", "wholesale_system_storecustomer") }}
    union all
    select 
        storeid as external_id, 
        modifieddate, 
        is_deleted, 
        date_partition,
        "wholesale_system_store" as table_source
    from {{ source("wholesale", "wholesale_system_store") }}
    union all
    select 
        employeeid as external_id, 
        modifieddate, 
        is_deleted, 
        date_partition,
        "hr_system_employee" as table_source
    from {{ source("hr_system", "hr_system_employee") }}
    union all
    select 
        vendorid as external_id, 
        modifieddate, 
        is_deleted, 
        date_partition,
        "product_management_platform_vendor" as table_source
    from {{ source("product", "product_management_platform_vendor") }}
)
select 
    row_number() over(order by CTE.external_id, CTE.table_source) as bussinessentityid,
    modifieddate, 
    is_deleted, 
    date_partition,
    table_source
from CTE

{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}

