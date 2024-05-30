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
    extract_date
from {{ source("production", "product_management_platform_transactionhistory") }}