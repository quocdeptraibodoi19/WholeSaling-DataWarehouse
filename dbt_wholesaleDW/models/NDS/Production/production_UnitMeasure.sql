{{ config(materialized='view') }}

select 
    unitmeasurecode,
    name,      
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_unitmeasure") }}