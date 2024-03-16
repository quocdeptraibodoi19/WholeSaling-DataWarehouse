{{ config(materialized='view') }}

select 
    productdescriptionid,
    description,        	                     	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productdescription") }}