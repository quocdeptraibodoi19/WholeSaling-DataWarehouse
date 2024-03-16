{{ config(materialized='view') }}

select 
    shoppingcartitemid,
    shoppingcartid,
    quantity,
    productid,
    datecreated,
    modifieddate,
    is_deleted,
    date_partition
from {{ source('ecomerce', 'ecomerce_shoppingcartitem') }}