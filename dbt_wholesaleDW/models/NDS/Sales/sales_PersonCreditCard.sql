{{ config(materialized='table') }}

with CTE as (
    select
        t.userid as bussinessentityid, 
        s.cardnumber,
        s.cardtype,
        s.expmonth,
        s.expyear
    from {{ source('ecomerce', 'ecomerce_usercreditcard') }} s
    inner join {{ ref("sales_CustomerOnlineUser") }} t
    on s.userid = t.old_userid
    union all
    select
        t.storerepid as bussinessentityid,
        s.cardnumber,
        s.cardtype,
        s.expmonth,
        s.expyear
    from {{ source('wholesale', 'wholesale_system_storerepcreditcard') }} s
    inner join {{ ref("sales_CustomerStoreUser") }} t
    on s.storerepid = t.old_storerepid
)
select
    CTE.bussinessentityid,
    s.creditcardid,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from CTE inner join {{ ref("sales_CreditCard") }} s
on 
    cte.cardnumber = s.cardnumber 
    and
    cte.cardtype = s.cardtype
    and
    cte.expmonth = s.expmonth
    and
    cte.expyear = s.expyear