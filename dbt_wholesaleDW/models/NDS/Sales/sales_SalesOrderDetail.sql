{{ 
    config(
        materialized='incremental',
        unique_key='sales_order_detail_id'
    ) 
}}

with ecom_saleorderheader_cte as (
    select 
        sales_order_id,
        old_salesorderid
    from {{ ref("sales_SalesOrderHeader") }} 
    where source = '{{ env_var("ecom_source") }}_user'
),
wholesale_saleorderheader_cte as (
    select 
        sales_order_id,
        old_salesorderid
    from {{ ref("sales_SalesOrderHeader") }} 
    where source = '{{ env_var("wholesale_source") }}_store'
),
ecomerce_salesorderdetail as (
    select
        t.sales_order_id,
        s.salesorderdetailid as old_salesorderdetailid,
        '{{ env_var("ecom_source") }}_user' as source,
        s.carriertrackingnumber as carrier_tracking_number,
        s.orderqty as order_qty,
        s.productid as product_id,
        s.specialofferid as special_offer_id,
        s.unitprice as unit_price,
        s.unitpricediscount as unit_price_discount,
        s.linetotal as line_total,
        s.modifieddate as updated_at,
        s.extract_date
    from {{ source("ecomerce", "ecomerce_salesorderdetail") }} s
    left join ecom_saleorderheader_cte t
    on s.salesorderid = t.old_salesorderid
),
wholesale_salesorderdetail as (
    select
        t.sales_order_id,
        s.salesorderdetailid as old_salesorderdetailid,
        '{{ env_var("wholesale_source") }}_store' as source,
        s.carriertrackingnumber as carrier_tracking_number,
        s.orderqty as order_qty,
        s.productid as product_id,
        s.specialofferid as special_offer_id,
        s.unitprice as unit_price,
        s.unitpricediscount as unit_price_discount,
        s.linetotal as line_total,
        s.modifieddate as updated_at,
        s.extract_date
    from {{ source("wholesale", "wholesale_system_salesorderdetail") }} s
    left join wholesale_saleorderheader_cte t
    on s.salesorderid = t.old_salesorderid
),
salesorderdetail as (
    select * from ecomerce_salesorderdetail
    union all
    select * from wholesale_salesorderdetail
)
select {{ dbt_utils.generate_surrogate_key(['sales_order_id', 'old_salesorderdetailid', 'source']) }} as sales_order_detail_id,
    *
from salesorderdetail
{% if is_incremental() %}

    where updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}