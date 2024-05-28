{{ config(materialized='view') }}

select 
    cultureid,
    name,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_culture") }}