{{ config(materialized='view') }}

select 
    scrapreasonid,
    `name`,      
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_scrapreason") }}