{{ config(materialized='view') }}

select 
    productmodelid,
    illustrationid,               	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productmodelillustration") }}