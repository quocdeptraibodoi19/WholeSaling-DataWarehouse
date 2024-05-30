{{ config(materialized='view') }}

select 
    productsubcategoryid,
    productcategoryid,   
    name,      
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productsubcategory") }}