{{ config(materialized='view') }}

select 
    productcategoryid,
    name,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productcategory") }}