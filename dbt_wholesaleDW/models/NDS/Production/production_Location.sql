{{ config(materialized='view') }}

select 
    locationid,
    `name`,
    costrate,
    `availability`,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_location") }}