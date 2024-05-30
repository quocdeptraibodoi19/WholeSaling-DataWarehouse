{{ config(materialized='view') }}

select 
    productmodelid,
    name,   
    catalogdescription,      
    instructions,               	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productmodel") }}