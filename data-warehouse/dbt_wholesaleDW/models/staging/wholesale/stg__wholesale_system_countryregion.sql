{{ config(materialized='view') }}

select
    dbt_scd_id as country_region_id,
    countryregioncode as country_code,
    countryregionname as country_name,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case
        when dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_countryregion_snapshot") }}