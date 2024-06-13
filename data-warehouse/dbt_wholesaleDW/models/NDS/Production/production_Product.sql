{{ 
    config(
        materialized='incremental',
        unique_key=['product_id', 'updated_at']
    ) 
}}

with production_product as (
    select *
    from {{ ref("stg__product_management_platform_product") }}
),

ecom_product as (
    select *
    from {{ ref("stg__ecomerce_product") }}
    where stg__ecomerce_product.product_name not in (
        select product_name from production_product
    )
),

wholesale_product as (
    select *
    from {{ ref("stg__wholesale_system_product") }}
    where stg__wholesale_system_product.product_name not in (
        select product_name from production_product
    )
),
cte_product as (
    select * from production_product
    union all
    select * from ecom_product
    union all
    select * from wholesale_product
)

select
    {{ dbt_utils.generate_surrogate_key(['cte_product.product_name']) }} as product_id,
    product_name,
    product_number,
    make_flag,
    finished_goods_flag,
    color,
    safety_stock_level,
    reorder_point,
    standard_cost,
    list_price,
    size,                     	
    size_unit_measure_code,               	
    weight_unit_measure_code,              	
    weight,                            	
    days_to_manufacture,                 	
    product_line,
    class,
    style,      	
    product_subcategory_id,              	
    product_model_id,                    	
    sell_start_date,                     	
    sell_end_date,                       	
    discontinued_date,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_deleted,
    is_valid
from cte_product

where 1 = 1
{% if is_incremental() %}

    and updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}