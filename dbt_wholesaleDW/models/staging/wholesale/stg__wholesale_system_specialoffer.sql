{{ config(materialized='view') }}

select
    specialofferid as special_offer_id,
    description,
    discountpct as discount_pct,
    type,
    category,
    from_unixtime(unix_timestamp(regexp_replace(startdate, ' {2,}', ' '), 'MMM dd yyyy hh:mma'), 'yyyy-MM-dd HH:mm:ss') as start_date,
    from_unixtime(unix_timestamp(regexp_replace(enddate, ' {2,}', ' '), 'MMM dd yyyy hh:mma'), 'yyyy-MM-dd HH:mm:ss') as end_date,
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
from {{ ref("wholesale_system_specialoffer_snapshot") }}