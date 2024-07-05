{{ 
    config(
        materialized='incremental',
        unique_key='sales_order_detail_id',
        file_format='delta',
        incremental_strategy='merge'
    ) 
}}

with ecomerce_salesorderdetail as (
    select
        s.salesorderid as sales_order_id,
        s.salesorderdetailid as old_salesorderdetailid,
        '{{ env_var("ecom_source") }}_user' as source,
        s.carriertrackingnumber as carrier_tracking_number,
        s.orderqty as order_qty,
       {{ dbt_utils.generate_surrogate_key(['k.name']) }} as product_id,
        t.special_offer_id,
        s.unitprice as unit_price,
        s.unitpricediscount as unit_price_discount,
        s.linetotal as line_total,
        s.modifieddate as updated_at,
        s.extract_date
    from {{ source("ecomerce", "ecomerce_salesorderdetail") }} s
    left join {{ ref("sales_SpecialOffer") }} as t
        on s.specialofferid = t.old_special_offer_id
            and t.source = '{{ env_var("ecom_source") }}'
    left join {{ source("ecomerce", "ecomerce_product") }} k
        on s.ProductID = k.ProductID
),
wholesale_salesorderdetail as (
    select
        s.salesorderid as sales_order_id,
        s.salesorderdetailid as old_salesorderdetailid,
        '{{ env_var("wholesale_source") }}_store' as source,
        s.carriertrackingnumber as carrier_tracking_number,
        s.orderqty as order_qty,
        {{ dbt_utils.generate_surrogate_key(['k.name']) }} as product_id,
        t.special_offer_id,
        s.unitprice as unit_price,
        s.unitpricediscount as unit_price_discount,
        s.linetotal as line_total,
        s.modifieddate as updated_at,
        s.extract_date
    from {{ source("wholesale", "wholesale_system_salesorderdetail") }} s
    left join {{ ref("sales_SpecialOffer") }} as t
        on s.specialofferid = t.old_special_offer_id
            and t.source = '{{ env_var("wholesale_source") }}'
    left join {{ source("wholesale", "wholesale_system_product") }} k
        on s.ProductID = k.ProductID
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