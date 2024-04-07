{{ config(materialized='view') }}

select 
    locationid,
    `name`,
    costrate,
    `availability`,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_location") }}