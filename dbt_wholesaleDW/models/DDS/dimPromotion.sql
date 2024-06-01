{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['sales_SpecialOffer.special_offer_id']) }} as promotion_key,
    sales_SpecialOffer.special_offer_id as promotion_id,
    sales_SpecialOffer.description as promotion_name,
    sales_SpecialOffer.discount_pct,
    sales_SpecialOffer.type,
    sales_SpecialOffer.category,
    sales_SpecialOffer.start_date,
    sales_SpecialOffer.end_date,
    sales_SpecialOffer.min_qty,
    sales_SpecialOffer.max_qty,
    case
        when sales_SpecialOffer.is_valid = 0 then 0
        else 1
    end as is_valid
from {{ ref('sales_SpecialOffer') }}