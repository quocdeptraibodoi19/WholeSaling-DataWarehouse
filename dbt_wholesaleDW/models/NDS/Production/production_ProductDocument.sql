{{ config(materialized='view') }}

select 
    productid,
    documentnode,   	                     	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productdocument") }}