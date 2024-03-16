{{ config(materialized='view') }}

select 
    cultureid,
    name,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_culture") }}