{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['production_Product.product_id']) }} as product_key,
    production_Product.product_id,
    production_Product.product_name,
    production_Product.product_number,
    production_Product.color,
    production_Product.days_to_manufacture,
	production_Product.safety_stock_level,
    production_Product.standard_cost,
    production_ProductSubcategory.product_subcategory_name,
    production_ProductCategory.product_category_name,
    production_Product.sell_start_date,
    production_Product.sell_end_date,
    case
        when production_Product.is_valid = 0 
            or production_ProductSubcategory.is_valid = 0
            or production_ProductCategory.is_valid = 0
            then 0
        else 1
    end as is_valid
from  {{ ref('production_Product') }}
left join  {{ ref('production_ProductSubcategory') }} 
    on production_Product.product_subcategory_id = production_ProductSubcategory.product_subcategory_id
left join  {{ ref('production_ProductCategory') }} 
    on production_ProductSubcategory.product_category_id = production_ProductCategory.product_category_id