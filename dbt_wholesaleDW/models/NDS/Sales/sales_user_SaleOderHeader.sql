{{ config(materialized='view') }}


with valid_address as (
    select * from {{ ref("person_Address") }}
    where is_valid = 1
),
billtoaddress_cte as (
    select 
        s.*,
        t.addressid as new_billtoaddressid
    from {{ source("ecomerce", "ecomerce_salesorderheader") }} s
    inner join valid_address t
    on s.billtoaddressid = t.old_addressid and t.source = '{{ env_var("ecom_source") }}_user'

),
shiptoaddress_cte as (
    select 
        s.*,
        t.addressid as new_shiptoaddressid
    from billtoaddress_cte s
    inner join valid_address t
    on s.shiptoaddressid = t.old_addressid and t.source = '{{ env_var("ecom_source") }}_user'
),
customer_mapping as (
    select
        s.customer_id,
        t.old_userid
    from {{ ref("sales_Customer") }} s
    inner join {{ ref("sales_CustomerOnlineUser") }} t
    on t.user_id = s.person_id and s.store_id is null
),
customer_cte as (
    select
        s.*,
        t.customer_id as new_customerid
    from shiptoaddress_cte s
    inner join customer_mapping t
    on s.userid = t.old_userid
),
creditcard_cte as (
    select
        s.*,
        t.credit_card_id as new_creditcardid
    from customer_cte s
    inner join {{ ref("sales_CreditCard") }} t
    on s.cardnumber = t.card_number and 
    s.cardtype = t.card_type and
    s.expmonth = t.exp_month and 
    s.expyear = t.exp_year
),
CTE_1 as (
    select 
        s.salesorderid as sales_order_id,
        s.revisionnumber as revision_number,
        s.orderdate as order_date,
        s.duedate as due_date,
        s.shipdate as ship_date,
        s.`status`,
        "1" as online_order_flag,
        s.salesordernumber as sales_order_number,
        CAST(NULL AS STRING) AS purchase_order_number,
        s.accountnumber as account_number,
        s.new_customerid as customer_id,
        CAST(NULL AS STRING) AS sales_person_id,
        s.territoryid as territory_id,
        s.new_billtoaddressid as bill_to_address_id,
        s.new_shiptoaddressid as ship_to_address_id,
        s.shipmethodid as ship_method_id,
        s.new_creditcardid as credit_card_id,
        s.creditcardapprovalcode as credit_card_approval_code,
        s.currencyrateid as currency_rate_id,
        s.subtotal as sub_total,
        s.taxamt as tax_amt,
        s.freight,
        s.totaldue as total_due,
        s.comment,
        s.modifieddate as updated_at,
        s.extract_date
    from creditcard_cte s
)
select * from CTE_1