{{ config(materialized='view') }}

select
    stateprovinceid as state_province_id,
    stateprovincecode as state_province_code,
    countryregioncode as country_region_code,
    isonlystateprovinceflag as is_only_state_province_flag,
    name as state_province_name,
    territoryid as territory_id,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case
        when dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_stateprovince_snapshot") }}