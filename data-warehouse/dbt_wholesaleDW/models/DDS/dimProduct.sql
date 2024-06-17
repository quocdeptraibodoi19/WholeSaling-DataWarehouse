{{ 
    config(
        materialized='incremental',
        unique_key=['product_key', 'dim_updated_at']
    ) 
}}

select
    {{ dbt_utils.generate_surrogate_key(['production_Product.product_id']) }} as product_key,
    production_Product.product_id,
    production_Product.product_name,
    production_Product.product_number,
    production_Product.color,
    CAST(production_Product.days_to_manufacture AS INT) AS days_to_manufacture,
    CAST(production_Product.safety_stock_level AS FLOAT) AS safety_stock_level,
    CAST(production_Product.standard_cost AS DECIMAL(10, 2)) AS standard_cost,
    production_ProductSubcategory.product_subcategory_name,
    production_ProductCategory.product_category_name,
    CAST(production_Product.sell_start_date AS DATE) AS sell_start_date,
    CAST(production_Product.sell_end_date AS DATE) AS sell_end_date,
    case
        when production_Product.is_valid = 0 
            or production_ProductSubcategory.is_valid = 0
            or production_ProductCategory.is_valid = 0
            then 0
        else 1
    end as is_valid,
    greatest(
        production_Product.updated_at,
        production_ProductSubcategory.updated_at,
        production_ProductCategory.updated_at
    ) as dim_updated_at

from  {{ ref('production_Product') }}
left join  {{ ref('production_ProductSubcategory') }} 
    on production_Product.product_subcategory_id = production_ProductSubcategory.product_subcategory_id
left join  {{ ref('production_ProductCategory') }} 
    on production_ProductSubcategory.product_category_id = production_ProductCategory.product_category_id

where 1 = 1
{% if is_incremental() %}

    and dim_updated_at >= ( select max(dim_updated_at) from {{ this }} )

{% endif %}