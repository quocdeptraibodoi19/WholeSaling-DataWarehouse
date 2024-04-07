{{ config(materialized='view') }}

select 
    transactionid,
    productid,
    referenceorderid,
    referenceorderlineid,
    transactiondate,
    transactiontype,
    quantity,
    actualcost,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_transactionhistory") }}