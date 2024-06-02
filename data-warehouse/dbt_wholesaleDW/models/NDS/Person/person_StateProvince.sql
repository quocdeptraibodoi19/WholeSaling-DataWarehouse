{{ config(materialized='view') }}

select
    state_province_id,
    state_province_code,
    country_region_code,
    is_only_state_province_flag,
    state_province_name,
    territory_id,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_valid
from {{ ref("stg__wholesale_system_stateprovince") }}