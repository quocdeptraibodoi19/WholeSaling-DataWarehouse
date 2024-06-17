{{ 
    config(
        materialized='incremental',
        unique_key='sales_key'
    ) 
}}

select
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.sales_order_id', 'sales_SalesOrderDetail.sales_order_detail_id']) }} as sales_key,
	sales_SalesOrderHeader.sales_order_id,
    sales_SalesOrderDetail.sales_order_detail_id,    
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderDetail.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.credit_card_id']) }} as creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.ship_to_address_id']) }} as ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.status']) }} as order_status_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.order_date_date']) }} as order_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.order_date_time']) }} as order_date_time_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.ship_date_date']) }} as ship_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.ship_date_time']) }} as ship_date_time_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.due_date_date']) }} as due_date_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderHeader.due_date_time']) }} as due_date_time_key,
    {{ dbt_utils.generate_surrogate_key(['sales_SalesOrderDetail.special_offer_id']) }} as promotion_key,
    sales_SalesOrderHeader.order_date,
    sales_SalesOrderHeader.ship_date,
    sales_SalesOrderHeader.due_date,
    CAST(sales_SalesOrderHeader.online_order_flag AS INT) AS is_online,
    CAST(sales_SalesOrderDetail.unit_price_discount AS DECIMAL(10, 2)) AS unit_price_discount,
    CAST(sales_SalesOrderDetail.unit_price AS DECIMAL(10, 2)) AS unit_price,
    CAST(sales_SalesOrderDetail.order_qty AS INT) AS order_qty,
    CAST(sales_SalesOrderDetail.line_total AS DECIMAL(10, 2)) AS sales_amount,
    case 
        when CAST(sales_SalesOrderDetail.unit_price_discount AS DECIMAL(10, 2)) > 0
            then CAST(sales_SalesOrderDetail.line_total AS DECIMAL(10, 2)) * CAST(sales_SalesOrderDetail.unit_price_discount AS DECIMAL(10, 2))
        else CAST(sales_SalesOrderDetail.line_total AS DECIMAL(10, 2))
    end as total_discount,
    sales_SalesOrderHeader.tax_amt,
    greatest(
        sales_SalesOrderHeader.updated_at,
        sales_SalesOrderDetail.updated_at
    ) as fct_updated_at

from {{ ref('sales_SalesOrderHeader') }}
inner join  {{ ref('sales_SalesOrderDetail') }} 
    on sales_SalesOrderHeader.old_salesorderid = sales_SalesOrderDetail.sales_order_id
        and sales_SalesOrderHeader.source = sales_SalesOrderDetail.source

where 1 = 1
{% if is_incremental() %}

    and fct_updated_at >= ( select max(fct_updated_at) from {{ this }} )

{% endif %}