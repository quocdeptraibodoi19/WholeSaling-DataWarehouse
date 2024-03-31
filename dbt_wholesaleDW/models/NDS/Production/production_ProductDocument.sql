{{ config(materialized='view') }}

select 
    productid,
    documentnode,   	                     	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productdocument") }}