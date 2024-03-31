{{ config(materialized='view') }}

with vendor_contact as (
    select
        t.bussinessentityid,
        k.bussinessentityid as personid,
        s.contacttypeid,
        s.modifieddate
        s.is_deleted,
        s.date_partition
    from {{ source("production", "product_management_platform_vendorcontact") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.vendorid = t.external_id and t.source = "vendor"
    inner join {{ ref("person_BussinessEntity") }} k
    on s.stackholderid = k.external_id and k.source = "stakeholder"
),
store_contact as (
    select
        t.bussinessentityid,
        k.bussinessentityid as personid,
        s.contacttypeid,
        s.modifieddate
        s.is_deleted,
        s.date_partition
    from {{ source("wholesale", "wholesale_system_storecontact") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.storeid = t.external_id and t.source = "store"
    inner join {{ ref("person_BussinessEntity") }} k
    on s.stackholderid = k.external_id and k.source = "stakeholder"
),
bussiness_contact as (
    select * from vendor_contact
    union all
    select * from store_contact
)
select * from bussiness_contact