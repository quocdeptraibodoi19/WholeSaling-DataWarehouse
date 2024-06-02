{{ config(materialized='view') }}

Select
    salesreasonid,
    name,
    reasontype,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('ecomerce', 'ecomerce_salesreason') }}  
