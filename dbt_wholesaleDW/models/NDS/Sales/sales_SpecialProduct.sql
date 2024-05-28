{{ config(materialized='view') }}

select 
    specialofferid,
    productid,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("ecomerce", "ecomerce_specialofferproduct") }}