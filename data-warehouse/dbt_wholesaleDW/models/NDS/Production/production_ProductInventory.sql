{{ config(materialized='view') }}

select 
    productid,
    locationid,   
    shelf,
    bin,
    quantity,     	                     	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productinventory") }}