{{ config(materialized='view') }}

select 
    productmodelid,
    name,   
    catalogdescription,      
    instructions,               	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productmodel") }}