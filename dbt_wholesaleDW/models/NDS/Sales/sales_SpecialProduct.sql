{{ config(materialized='view') }}

select 
    specialofferid,
    productid,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("ecomerce", "ecomerce_specialofferproduct") }}