{{ config(materialized='incremental') }}

with billtoaddress_cte as (
    select 
        s.*,
        t.addressid as new_billtoaddressid
    from {{ source("wholesale", "wholesale_system_salesorderheader") }} s
    inner join {{ ref("person_Address") }} t
    on s.billtoaddressid = t.old_addressid and t.source = '{{ env_var("wholesale_source") }}_store'
),
shiptoaddress_cte as (
    select 
        s.*,
        t.addressid as new_shiptoaddressid
    from billtoaddress_cte s
    inner join {{ ref("person_Address") }} t
    on s.shiptoaddressid = t.old_addressid and t.source = '{{ env_var("wholesale_source") }}_store'
),
employee_cte as (
    select
        s.*,
        t.bussiness_entity_id as new_salespersonid
    from shiptoaddress_cte s
    inner join {{ ref("hr_Employee") }} t
    on t.national_id_number = s.saleemployeenationalnumberid
),
creditcard_cte as (
    select
        s.*,
        t.credit_card_id as new_creditcardid
    from employee_cte s
    inner join {{ ref("sales_CreditCard") }} t
    on s.creditcardid = t.old_credit_card_id and t.source = '{{ env_var("wholesale_source") }}_store'
),
CTE_1 as (
    select 
        s.salesorderid as sales_order_id,
        s.revisionnumber as revision_number,
        s.orderdate as order_date,
        s.duedate as due_date,
        s.shipdate as ship_date,
        s.`status`,
        "0" as online_order_flag,
        s.salesordernumber as sales_order_number,
        s.purchaseordernumber as purchase_order_number,
        s.accountnumber as account_number,
        s.customerid as customer_id,
        s.new_salespersonid as sales_person_id,
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
        CAST(NULL AS STRING) AS comment,
        s.modifieddate as updated_at,
        s.extract_date
    from creditcard_cte s
)
select * from CTE_1
{% if is_incremental() %}

    where updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}