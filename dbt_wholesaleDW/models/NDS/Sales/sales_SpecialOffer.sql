{{ config(materialized='view') }}

-- We can still use: SELECT * FROM {{ source('wholesale', 'wholesale_system_specialoffer') }}
select 
    specialofferid,
    description,
    discountpct,
    type,
    category,
    startdate,
    enddate,
    minqty,
    maxqty,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('ecomerce', 'ecomerce_specialoffer') }}
