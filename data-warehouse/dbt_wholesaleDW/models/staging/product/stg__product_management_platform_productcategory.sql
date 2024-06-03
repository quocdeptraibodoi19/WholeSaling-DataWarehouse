{{ config(materialized='view') }}

select
    productcategoryid as product_category_id,
    name as product_category_name,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case
        when dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("product_management_platform_productcategory_snapshot") }}