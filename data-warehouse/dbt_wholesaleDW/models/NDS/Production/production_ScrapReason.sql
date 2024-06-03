{{ config(materialized='view') }}

select 
    scrapreasonid,
    `name`,      
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_scrapreason") }}