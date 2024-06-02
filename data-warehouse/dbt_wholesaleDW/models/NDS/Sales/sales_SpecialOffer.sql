{{ config(materialized='table') }}

with ecom_special_offer as (
    select 
        special_offer_id,
        description,
        discount_pct,
        type,
        category,
        start_date,
        end_date,
        min_qty,
        max_qty,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_valid,
        '{{ env_var("ecom_source") }}' as source
    from {{ ref('stg__ecomerce_specialoffer') }}
),

wholesale_special_offer as (
    select 
        special_offer_id,
        description,
        discount_pct,
        type,
        category,
        start_date,
        end_date,
        min_qty,
        max_qty,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_valid,
        '{{ env_var("wholesale_source") }}' as source
    from {{ ref('stg__wholesale_system_specialoffer') }}
),

special_offer_cte as (
    select * from ecom_special_offer
    union all
    select * from wholesale_special_offer
)

select
    {{ dbt_utils.generate_surrogate_key(['special_offer_id', 'source']) }} as special_offer_id,
    special_offer_id as old_special_offer_id,
    description,
    discount_pct,
    type,
    category,
    start_date,
    end_date,
    min_qty,
    max_qty,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_valid,
    source
from special_offer_cte

