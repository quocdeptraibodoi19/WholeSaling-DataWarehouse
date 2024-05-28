{{ config(materialized='view') }}

select 
    productid,
    productphotoid,   
    `primary`,          	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productproductphoto") }}