{{ config(materialized='view') }}

with id_saleorderheader_cte as (
    select
        salesorderid,
        old_salesorderid
    from {{ ref("sales_SalesOrderHeader") }}
    where source = "ecom_user"
),
mapping_saleorderreasons_cte as (
    select
        t.salesorderid,
        s.salesreasonid,
        s.modifieddate,
        s.is_deleted,
        s.extract_date
    from {{ source("ecomerce", "ecomerce_salesorderheadersalesreason") }} s
    inner join id_saleorderheader_cte t
    on s.salesorderid = t.old_salesorderid
)
select * from mapping_saleorderreasons_cte