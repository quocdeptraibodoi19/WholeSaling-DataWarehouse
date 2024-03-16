{{ config(materialized='view') }}

with CTE as ( 
    select
        userid as personid,
        null as storeid,
        accountnumber,
        modifieddate,
        is_deleted,
        date_partition
    from {{ ref("sales_CustomerOnlineUser") }} 
    union all
    select 
        storerepid as personid,
        storeid,
        accountnumber,
        modifieddate,
        is_deleted,
        date_partition
    from {{ ref("sales_CustomerStoreUser") }}
)
select 
    row_number() over (order by personid, storeid, accountnumber) as customerid,
    CTE.*
from CTE