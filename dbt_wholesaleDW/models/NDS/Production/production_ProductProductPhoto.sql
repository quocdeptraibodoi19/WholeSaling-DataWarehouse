{{ config(materialized='view') }}

select 
    productid,
    productphotoid,   
    primary,          	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productproductphoto") }}