{{ config(materialized='view') }}

with purchase_order_header as (
    select
        purchaseorderid,
        revisionnumber,
        `status`,
        k.bussinessentityid as employeeid,
        vendorid,
        shipmethodid,
        orderdate,
        shipdate,
        subtotal,
        taxamt,
        freight,
        totaldue,
        modifieddate,
        is_deleted,
        date_partition
    from {{ source("production", "product_management_platform_purchaseorderheader") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.vendorid = t.external_id and t.source = 'vendor'
    inner join {{ ref("hr_Employee") }} k
    on s.employeenationalidnumber = k.nationalidnumber
)
select * from purchase_order_header