{{ config(materialized='view') }}

select 
    productid,     	
    vendorid,
    averageleadtime,
    standardprice,
    lastreceiptcost,
    lastreceiptdate,
    minorderqty,
    maxorderqty,
    onorderqty,
    unitmeasurecode,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productvendor") }}