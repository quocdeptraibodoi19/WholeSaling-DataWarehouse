{{ config(materialized='view') }}

select 
    productid,
    startdate,   
    enddate,
    listprice,	                     	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productlistpricehistory") }}