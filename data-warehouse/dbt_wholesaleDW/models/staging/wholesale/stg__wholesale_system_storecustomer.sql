{{ config(materialized='view') }}

select
    customerid as customer_id,
    storerepid as store_rep_id,
    storeid as store_id,
    territoryid as territory_id,
    accountnumber as account_number,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_storecustomer_snapshot") }}