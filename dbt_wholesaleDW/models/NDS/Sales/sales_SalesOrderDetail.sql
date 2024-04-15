{{ config(materialized='table') }}

with ecom_saleorderheader_cte as (
    select 
        salesorderid,
        old_salesorderid
    from {{ ref("sales_SalesOrderHeader") }} 
    where source = "ecom_user" 
),
wholesale_saleorderheader_cte as (
    select 
        salesorderid,
        old_salesorderid
    from {{ ref("sales_SalesOrderHeader") }} 
    where source = "wholesale" 
),
ecomerce_salesorderdetail as (
    select
        t.salesorderid,
        s.salesorderdetailid as old_salesorderdetailid,
        "ecom_user" as source,
        s.carriertrackingnumber,
        s.orderqty,
        s.productid,
        s.specialofferid,
        s.unitprice,
        s.unitpricediscount,
        s.linetotal,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("ecomerce", "ecomerce_salesorderdetail") }} s
    inner join ecom_saleorderheader_cte t
    on s.salesorderid = t.old_salesorderid
),
wholesale_salesorderdetail as (
    select
        t.salesorderid,
        s.salesorderdetailid as old_salesorderdetailid,
        "wholesale" as source,
        s.carriertrackingnumber,
        s.orderqty,
        s.productid,
        s.specialofferid,
        s.unitprice,
        s.unitpricediscount,
        s.linetotal,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("wholesale", "wholesale_system_salesorderdetail") }} s
    inner join wholesale_saleorderheader_cte t
    on s.salesorderid = t.old_salesorderid
),
salesorderdetail as (
    select * from ecomerce_salesorderdetail
    union all
    select * from wholesale_salesorderdetail
)
select *,
    row_number() over (order by salesorderid, old_salesorderdetailid, source) as salesorderdetailid
from salesorderdetail