{{ config(materialized='view') }}

select
    {{ dbt_utils.generate_surrogate_key(['sales_SpecialOffer.special_offer_id']) }} as promotion_key,
    sales_SpecialOffer.special_offer_id as promotion_id,
    sales_SpecialOffer.description as promotion_name,
    CAST(sales_SpecialOffer.discount_pct AS DECIMAL(5, 2)) AS discount_pct,
    sales_SpecialOffer.type,
    sales_SpecialOffer.category,
    CAST(sales_SpecialOffer.start_date AS DATE) AS start_date,
    CAST(sales_SpecialOffer.end_date AS DATE) AS end_date,
    CAST(sales_SpecialOffer.min_qty AS INT) AS min_qty,
    CAST(sales_SpecialOffer.max_qty AS INT) AS max_qty
from {{ ref('sales_SpecialOffer') }}
where sales_SpecialOffer.is_valid != 0
