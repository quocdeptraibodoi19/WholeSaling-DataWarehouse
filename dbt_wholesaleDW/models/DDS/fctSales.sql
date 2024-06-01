{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.sales_order_id', 'sales_SalesOrderDetail.sales_order_detail_id']) }} as sales_key,
	sales_SalesOrderHeader.sales_order_id,
    sales_SalesOrderDetail.sales_order_detail_id,    
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderDetail.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.credit_card_id']) }} as creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.ship_to_address_id']) }} as ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.status']) }} as order_status_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.order_date']) }} as order_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.ship_date']) }} as ship_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.due_date']) }} as due_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderDetail.specialofferid']) }} as promotion_key,
    sales_SalesOrderHeader.online_order_flag as is_online,
    sales_SalesOrderDetail.unit_price_discount as unit_price_discount,
    sales_SalesOrderHeader.sales_order_number,
    sales_SalesOrderDetail.unit_price,
    sales_SalesOrderDetail.order_qty,
    sales_SalesOrderDetail.line_total as sales_amount,
    case when sales_SalesOrderDetail.unit_price_discount > 0
        then sales_SalesOrderDetail.line_total * sales_SalesOrderDetail.unit_price_discount 
        else sales_SalesOrderDetail.line_total
        end as total_discount,
    sales_SalesOrderHeader.tax_amt 
from {{ ref('sales_SalesOrderHeader') }}
inner join  {{ ref('sales_SalesOrderDetail') }} 
    on sales_SalesOrderHeader.old_salesorderid = sales_SalesOrderDetail.sales_order_id
        and sales_SalesOrderHeader.source = sales_SalesOrderDetail.source