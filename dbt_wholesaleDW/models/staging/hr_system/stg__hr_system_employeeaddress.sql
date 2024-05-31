{{ config(materialized='view') }}

select 
    dbt_scd_id as employee_address_id,
    addressid as old_addressid,
    addresstypeid,
    addressline1,
    addressline2,
    stateprovinceid,
    postalcode,
    spatiallocation,
    employeeid,
    extract_date,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("hr_system_employeeaddress_snapshot") }}