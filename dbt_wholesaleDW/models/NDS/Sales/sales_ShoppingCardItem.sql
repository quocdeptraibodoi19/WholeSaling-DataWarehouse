{{ config(materialized='view') }}

select 
    shoppingcartitemid,
    shoppingcartid,
    quantity,
    productid,
    datecreated,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('ecomerce', 'ecomerce_shoppingcartitem') }}