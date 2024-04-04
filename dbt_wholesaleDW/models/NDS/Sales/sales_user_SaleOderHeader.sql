{{ config(materialized='view') }}

with billtoaddress_cte as (
    select 
        s.*,
        t.addressid as new_billtoaddressid,
    from {{ source("ecomerce", "ecomerce_salesorderheader") }} s
    inner join {{ ref("person_Address") }} t
    on s.billtoaddressid = t.old_addressid and t.source = "ecom_user"

),
shiptoaddress_cte as (
    select 
        s.*,
        t.addressid as new_shiptoaddressid,
    from billtoaddress_cte s
    inner join {{ ref("person_Address") }} t
    on s.shiptoaddressid = t.old_addressid and t.source = "ecom_user"
),
customer_mapping as (
    select
        s.customerid,
        t.old_userid
    from {{ ref("sales_Customer") }} s
    inner join {{ ref("sales_CustomerOnlineUser") }} t
    on t.userid = s.personid and s.storeid is null
),
customer_cte as (
    select
        s.*,
        t.customerid as new_customerid
    from shiptoaddress_cte s
    inner join customer_mapping t
    on s.userid = t.old_userid
),
creditcard_cte as (
    select
        s.*,
        t.creditcardid as new_creditcardid
    from customer_cte s
    inner join {{ ref("sales_CreditCard") }} t
    on s.cardnumber = t.cardnumber and 
    s.cardtype = t.cardtype and
    s.expmonth = t.expmonth and 
    s.expyear = t.expyear
)
select 
    s.salesorderid,
    s.revisionnumber,
    s.orderdate,
    s.duedate,
    s.shipdate,
    s.`status`,
    "1" as onlineorderflag,
    s.salesordernumber,
    null as purchaseordernumber,
    s.accountnumber,
    s.new_customerid as customerid,
    null as salespersonid,
    s.territoryid,
    s.new_billtoaddressid as billtoaddressid,
    s.new_shiptoaddressid as shiptoaddressid,
    s.shipmethodid,
    s.new_creditcardid as creditcardid,
    s.creditcardapprovalcode,
    s.currencyrateid,
    s.subtotal,
    s.taxamt,
    s.freight,
    s.totaldue,
    s.comment,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from creditcard_cte s