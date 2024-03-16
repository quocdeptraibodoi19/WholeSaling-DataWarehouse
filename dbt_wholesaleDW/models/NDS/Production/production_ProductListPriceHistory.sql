{{ config(materialized='view') }}

select 
    productid,
    startdate,   
    enddate,
    listprice,	                     	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productlistpricehistory") }}