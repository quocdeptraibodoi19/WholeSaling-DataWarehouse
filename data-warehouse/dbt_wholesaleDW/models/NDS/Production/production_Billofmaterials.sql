{{ config(materialized='view') }}

select 
    billofmaterialsid,
    productassemblyid,
    componentid,
    startdate,
    enddate,
    unitmeasurecode,
    bomlevel,
    perassemblyqty,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_billofmaterials") }}