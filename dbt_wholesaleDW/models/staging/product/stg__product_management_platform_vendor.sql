{{ config(materialized='view') }}

select
    vendorid as vendor_id,
    accountnumber as account_number,
    name as vendor_name,
    creditrating as credit_rating,
    preferredvendorstatus as preferred_vendor_status,
    activeflag as active_flag,
    purchasingwebServiceurl as purchasing_webService_url,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("product_management_platform_vendor_snapshot") }}