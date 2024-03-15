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
    date_partition
from {{ source("product", "product_management_platform_vendor") }}