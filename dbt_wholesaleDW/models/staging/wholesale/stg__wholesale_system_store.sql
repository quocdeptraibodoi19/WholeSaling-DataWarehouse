{{ config(materialized='view') }}

select
    storeid as store_id,
    name as store_name,
    employeenationalidnumber as employee_national_id_number,
    demographics,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_store_snapshot") }}