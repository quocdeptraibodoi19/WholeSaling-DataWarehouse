{{ config(materialized='view') }}

select 
    productmodelid,
    productdescriptionid,    
    cultureid,           	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productmodelproductdescriptionculture") }}