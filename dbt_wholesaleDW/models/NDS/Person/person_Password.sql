{{ config(materialized='view') }}

with online_user_password as (
    select
        t.bussinessentityid,
        s.passwordhash,
        s.passwordsalt,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("ecomerce", "ecomerce_userpassword") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on t.source = "ecom_user" and t.external_id = s.userid
),
employee_password as (
    select
        t.bussinessentityid,
        s.passwordhash,
        s.passwordsalt,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_employee") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on t.source = "employee" and t.external_id = s.employeeid
),
stakeholder_password as (
    select
        t.bussinessentityid,
        s.passwordhash,
        s.passwordsalt,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_stackholderpassword") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on t.source = "stakeholder" and t.external_id = s.stackholderid
),
password as (
    select * from online_user_password
    union all
    select * from employee_password
    union all
    select * from stakeholder_password
)
select * from password