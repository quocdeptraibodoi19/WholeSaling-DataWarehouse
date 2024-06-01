{{ config(materialized='table') }}

with cte as (
    select 
        *,
        '{{ env_var("ecom_source") }}_user' as source
    from {{ ref("sales_user_SaleOderHeader") }}
    union all
    select 
        *,
        '{{ env_var("wholesale_source") }}_store' as source
    from {{ ref("sales_wholesale_SaleOrderHeader") }}
),
CTE_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['sales_order_id', 'source']) }} as sales_order_id,
        s.sales_order_id as old_salesorderid,
        s.source,
        s.revision_number,
        s.order_date,
        s.due_date,
        s.ship_date,
        s.`status`,
        s.online_order_flag,
        s.sales_order_number,
        s.purchase_order_number,
        s.account_number,
        s.customer_id,
        s.sales_person_id,
        s.territory_id,
        s.bill_to_address_id,
        s.ship_to_address_id,
        s.ship_method_id,
        s.credit_card_id,
        s.credit_card_approval_code,
        s.currency_rate_id,
        s.sub_total,
        s.tax_amt,
        s.freight,
        s.total_due,
        s.comment,
        s.updated_at,
        s.extract_date
    from cte s
)
select * from CTE_1
{% if is_incremental() %}

    where updated_at >= ( select max(updated_at) from {{ this }} )
    
{% endif %}
