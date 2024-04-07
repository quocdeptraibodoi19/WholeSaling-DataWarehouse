{{ config(materialized='table') }}

with billtoaddress_cte as (
    select 
        s.*,
        t.addressid as new_billtoaddressid
    from {{ source("wholesale", "wholesale_system_salesorderheader") }} s
    inner join {{ ref("person_Address") }} t
    on s.billtoaddressid = t.old_addressid and t.source = "store"
),
shiptoaddress_cte as (
    select 
        s.*,
        t.addressid as new_shiptoaddressid
    from billtoaddress_cte s
    inner join {{ ref("person_Address") }} t
    on s.shiptoaddressid = t.old_addressid and t.source = "store"
),
customer_mapping as (
    select
        s.customerid,
        t.old_storerepid,
        t.old_storeid
    from {{ ref("sales_Customer") }} s
    inner join {{ ref("sales_CustomerStoreUser") }} t
    on s.personid = t.storerepid and s.storeid = t.storeid
),
customer_cte as (
    select
        s.*,
        t.customerid as new_customerid
    from shiptoaddress_cte s
    inner join customer_mapping t
    on s.personid = t.old_storerepid and s.storeid = t.old_storeid
),
employee_cte as (
    select
        s.*,
        t.bussinessentityid as new_salespersonid
    from customer_cte s
    inner join {{ ref("hr_Employee") }} t
    on t.nationalidnumber = s.saleemployeenationalnumberid
),
creditcard_cte as (
    select
        s.*,
        t.creditcardid as new_creditcardid
    from employee_cte s
    inner join {{ ref("sales_CreditCard") }} t
    on s.creditcardid = t.old_creditcardid and t.source = "wholesale"
)
select 
    s.salesorderid,
    s.revisionnumber,
    s.orderdate,
    s.duedate,
    s.shipdate,
    s.`status`,
    "0" as onlineorderflag,
    s.salesordernumber,
    s.purchaseordernumber,
    s.accountnumber,
    s.new_customerid as customerid,
    s.new_salespersonid as salespersonid,
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
    CAST(NULL AS STRING) AS comment,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from creditcard_cte s
