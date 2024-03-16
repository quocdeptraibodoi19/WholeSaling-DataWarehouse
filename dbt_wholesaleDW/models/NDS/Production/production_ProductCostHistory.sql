{{ config(materialized='view') }}

select 
    productid,
    startdate,        	               
    enddate,              	               
    standardcost,        	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productcosthistory") }}