{{ config(materialized='view') }}

select
    purchaseorderid,
    purchaseorderdetailid,
    duedate,
    orderqty,
    productid,
    unitprice,
    linetotal,
    receivedqty,
    rejectedqty,
    stockedqty,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_purchaseorderdetail") }}