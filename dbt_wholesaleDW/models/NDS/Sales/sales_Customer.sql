{{ config(materialized='view') }}

select
    row_number() over(order by userid, accountnumber) as customerid,
    userid as personid,
    "" as storeid,
    accountnumber,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("ecomerce", "ecomerce_user") }} 
union all
select 
    row_number() over(order by customerid, storeid, storerepid, accountnumber) as customerid,
    storerepid as personid,
    storeid,
    accountnumber,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("wholesale", "wholesale_system_storecustomer") }}