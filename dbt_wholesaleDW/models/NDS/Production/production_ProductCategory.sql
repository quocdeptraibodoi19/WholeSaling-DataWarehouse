{{ config(materialized='view') }}

select 
    productcategoryid,
    name,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productcategory") }}