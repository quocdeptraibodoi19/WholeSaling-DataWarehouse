{{ config(materialized='view') }}

select 
    shipmethodid,
    name,
    shipbase,
    shiprate,
    rowguid,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_shipmethod") }}