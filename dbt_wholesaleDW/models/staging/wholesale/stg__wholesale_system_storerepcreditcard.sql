{{ config(materialized='view') }}

select
    storerepid as store_rep_id,
    creditcardid as credit_card_id,
    cardnumber as card_number,
    cardtype as card_type,
    expmonth as exp_month,
    expyear as exp_year,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_storerepcreditcard_snapshot") }}