{{ config(materialized='view') }}

select 
    productdescriptionid,
    description,        	                     	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productdescription") }}