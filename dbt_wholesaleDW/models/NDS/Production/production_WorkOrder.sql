{{ config(materialized='view') }}

select 
    workorderid,     	
    productid,
    orderqty,
    stockedqty,
    scrappedqty,
    startdate,
    enddate,
    duedate,
    scrapreasonid,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_workorder") }}