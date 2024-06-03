{{ config(materialized='view') }}

select
    specialofferid as special_offer_id,
    productid as product_id,
    extract_date
from {{ source("wholesale", "wholesale_system_specialofferproduct") }}