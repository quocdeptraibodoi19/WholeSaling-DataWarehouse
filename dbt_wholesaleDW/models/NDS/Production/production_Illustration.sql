{{ config(materialized='view') }}

select 
    illustrationid,
    diagram,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_illustration") }}