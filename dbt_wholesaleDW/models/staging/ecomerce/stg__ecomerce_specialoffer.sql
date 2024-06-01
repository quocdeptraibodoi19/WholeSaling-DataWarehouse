{{ config(materialized='view') }}

select
    specialofferid as special_offer_id,
    description,
    discountpct as discount_pct,
    type,
    category,
    startdate as start_date,
    enddate as end_date,
    minqty as min_qty,
    maxqty as max_qty,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case
        when dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("ecomerce_specialoffer_snapshot") }}