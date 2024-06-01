{{ config(materialized='table') }}

with ecom_special_product as (
    select 
        special_offer_id,
        product_id,
        extract_date,
        '{{ env_var("ecom_source") }}' as source
    from {{ ref('stg__ecomerce_specialofferproduct') }}
),

wholesale_special_product as (
    select 
        special_offer_id,
        product_id,
        extract_date,
        '{{ env_var("wholesale_source") }}' as source
    from {{ ref('stg__wholesale_system_specialofferproduct') }}
),

special_product_cte as (
    select * from ecom_special_product
    union all
    select * from wholesale_special_product
)

select
    {{ dbt_utils.generate_surrogate_key(['special_offer_id', 'product_id', 'source']) }} as special_product_id,
    special_offer_id,
    product_id,
    extract_date,
    source
from special_product_cte
