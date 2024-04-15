{{ config(materialized='table') }}

with cte as (
    select 
        *,
        "ecom_user" as source
    from {{ ref("sales_user_SaleOderHeader") }}
    union all
    select 
        *,
        "wholesale" as source
    from {{ ref("sales_wholesale_SaleOrderHeader") }}
),
CTE_1 as (
    select 
        row_number() over (order by salesorderid, source) as salesorderid,
        s.salesorderid as old_salesorderid,
        s.source,
        s.revisionnumber,
        s.orderdate,
        s.duedate,
        s.shipdate,
        s.`status`,
        s.onlineorderflag,
        s.salesordernumber,
        s.purchaseordernumber,
        s.accountnumber,
        s.customerid,
        s.salespersonid,
        s.territoryid,
        s.billtoaddressid,
        s.shiptoaddressid,
        s.shipmethodid,
        s.creditcardid,
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
    from cte s
)
select * from CTE_1
{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )
    
{% endif %}
