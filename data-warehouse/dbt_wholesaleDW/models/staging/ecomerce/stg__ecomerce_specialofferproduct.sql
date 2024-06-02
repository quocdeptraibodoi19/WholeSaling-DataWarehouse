{{ config(materialized='view') }}

select
    specialofferid as special_offer_id,
    productid as product_id,
    extract_date
from  {{ source("ecomerce", "ecomerce_specialofferproduct") }}