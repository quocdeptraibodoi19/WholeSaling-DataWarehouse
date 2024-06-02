{{ config(materialized='view') }}

select 
    productmodelid,
    illustrationid,               	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productmodelillustration") }}