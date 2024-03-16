{{ config(materialized='view') }}

select 
    productmodelid,
    productdescriptionid,    
    cultureid,           	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productmodelproductdescriptionculture") }}