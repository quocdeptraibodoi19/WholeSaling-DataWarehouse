{{ config(materialized='view') }}

-- Wait for the bussiness entity to be constructed.

select 
    vendorid,
    accountnumber,
    name,
    creditrating,
    preferredvendorstatus,
    activeflag,
    purchasingwebserviceurl,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_vendor") }}