{{ config(materialized='view') }}

select 
    address_type_id,
    address_type_name,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_valid
from {{ ref("stg__hr_system_addresstype") }}