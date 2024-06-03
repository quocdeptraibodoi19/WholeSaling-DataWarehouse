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
    extract_date
from {{ source("production", "product_management_platform_workorder") }}