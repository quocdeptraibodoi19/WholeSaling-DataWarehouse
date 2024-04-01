{{ config(materialized='view') }}

with user_phonecontact as (
    select
        t.bussinessentityid,
        s.phonenumber,
        s.phonenumbertypeid,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("ecomerce", "ecomerce_userphonecontact") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.userid = t.external_id and t.source = "ecom_user"
),
employee_phonecontact as (
    select
        t.bussinessentityid,
        s.phonenumber,
        s.phonenumbertypeid,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_employeephonecontact") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.userid = t.external_id and t.source = "employee"
),
stackholder_phonecontact as (
select
        t.bussinessentityid,
        s.phonenumber,
        s.phonenumbertypeid,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_stackholderphonecontact") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.userid = t.external_id and t.source = "stakeholder"
),
person_phone as (
    select * from user_phonecontact
    union all
    select * from employee_phonecontact
    union all
    select * from stackholder_phonecontact
)
select * from person_phone