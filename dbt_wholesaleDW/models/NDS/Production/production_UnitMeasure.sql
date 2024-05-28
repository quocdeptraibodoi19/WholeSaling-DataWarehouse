{{ config(materialized='view') }}

select 
    unitmeasurecode,
    name,      
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_unitmeasure") }}