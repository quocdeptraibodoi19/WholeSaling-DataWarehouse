{{ config(materialized='view') }}

select 
    product_subcategory_id,
    product_category_id,   
    product_subcategory_name,      
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_valid
from {{ ref("stg__product_management_platform_productsubcategory") }}