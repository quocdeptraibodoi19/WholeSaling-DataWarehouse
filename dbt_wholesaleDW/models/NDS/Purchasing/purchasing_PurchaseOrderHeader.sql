{{ config(materialized='table') }}

with purchase_order_header as (
    select
        s.purchaseorderid,
        s.revisionnumber,
        s.`status`,
        k.bussinessentityid as employeeid,
        s.vendorid,
        s.shipmethodid,
        s.orderdate,
        s.shipdate,
        s.subtotal,
        s.taxamt,
        s.freight,
        s.totaldue,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("production", "product_management_platform_purchaseorderheader") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.vendorid = t.external_id and t.source = 'vendor'
    inner join {{ ref("hr_Employee") }} k
    on s.employeenationalidnumber = k.nationalidnumber
)
select * from purchase_order_header