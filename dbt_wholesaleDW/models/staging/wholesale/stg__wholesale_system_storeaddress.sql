{{ config(materialized='view') }}

select
    dbt_scd_id as store_address_id,
    addressid as old_addressid,
    addresstypeid as address_type_id,
    addressline1,
    addressline2,
    stateprovinceid as state_province_id,
    postalcode as postal_code,
    spatiallocation as spatial_location,
    storeid as store_id,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("wholesale_system_storeaddress_snapshot") }}