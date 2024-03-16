{{ config(materialized='view') }}

select 
    illustrationid,
    diagram,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_illustration") }}