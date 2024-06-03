{{ config(materialized='view') }}

select 
    productid as product_id,
    name as product_name,
    productnumber as product_number,
    makeflag as make_flag,
    finishedgoodsflag as finished_goods_flag,
    color,
    safetystocklevel as safety_stock_level,
    reorderpoint as reorder_point,
    standardcost as standard_cost,
    listprice as list_price,
    size,                     	
    sizeunitmeasurecode as size_unit_measure_code,               	
    weightunitmeasurecode as weight_unit_measure_code,         	
    weight,                            	
    daystomanufacture as days_to_manufacture,                 	
    productline as product_line,
    class,
    style,      	
    productsubcategoryid as product_subcategory_id,              	
    productmodelid as product_model_id,                    	
    sellstartdate as sell_start_date,                     	
    sellenddate as sell_end_date,                       	
    discontinueddate as discontinued_date,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("product_management_platform_product_snapshot") }}