{{ config(materialized='view') }}

Select
    salesreasonid,
    name,
    reasontype,
    modifieddate,
    is_deleted,
    date_partition
from {{ source('ecomerce', 'ecomerce_salesreason') }}  
