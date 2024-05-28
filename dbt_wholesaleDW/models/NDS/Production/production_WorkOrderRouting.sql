{{ config(materialized='view') }}

select 
    workorderid,     	
    productid,
    operationsequence,
    locationid,
    scheduledstartdate,
    scheduledenddate,
    actualstartdate,
    actualenddate,
    actualresourcehrs,
    plannedcost,
    actualcost,  	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_workorderrouting") }}