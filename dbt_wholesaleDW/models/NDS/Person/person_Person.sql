{{ config(materialized='view') }}

with online_user as (
    select
        t.bussinessentityid,
        "IN" as persontype,
        s.namestyle,
        s.title,
        s.firstname,
        s.middlename,
        s.lastname,
        s.suffix,
        s.emailpromotion,
        s.additionalcontactinfo,
        s.demographics,
        s.birthdate,
        s.maritalstatus,
        s.gender,
        s.totalchildren,
        s.numberchildrenathome,
        s.houseownerflag,
        s.numbercarsowned,
        s.datefirstpurchase,
        s.commutedistance,
        s.education,
        s.occupation,
        s.modifieddate,
        s.is_deleted,
        s.extract_date
    from {{ source("ecomerce", "ecomerce_user") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.userid = t.external_id and t.source = "ecom_user"
),
employee as (
    select
        t.bussinessentityid,
        s.persontype,
        s.namestyle,
        s.title,
        s.firstname,
        s.middlename,
        s.lastname,
        s.suffix,
        s.emailpromotion,
        s.additionalcontactinfo,
        s.demographics,
        s.birthdate,
        s.maritalstatus,
        s.gender,
        s.totalchildren,
        s.numberchildrenathome,
        s.houseownerflag,
        s.numbercarsowned,
        s.datefirstpurchase,
        s.commutedistance,
        s.education,
        s.occupation,
        s.modifieddate,
        s.is_deleted,
        s.extract_date
    from {{ source("hr_system", "hr_system_employee") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.employeeid = t.external_id and t.source = "employee"
),
stackeholder as (
    select
        t.bussinessentityid,
        s.persontype,
        s.namestyle,
        s.title,
        s.firstname,
        s.middlename,
        s.lastname,
        s.suffix,
        s.emailpromotion,
        s.additionalcontactinfo,
        s.demographics,
        s.birthdate,
        s.maritalstatus,
        s.gender,
        s.totalchildren,
        s.numberchildrenathome,
        s.houseownerflag,
        s.numbercarsowned,
        s.datefirstpurchase,
        s.commutedistance,
        s.education,
        s.occupation,
        s.modifieddate,
        s.is_deleted,
        s.extract_date
    from {{ source("hr_system", "hr_system_stakeholder") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.stackholderid = t.external_id and t.source = "stakeholder"
),
person as (
    select * from online_user
    union all
    select * from employee
    union all
    select * from stackeholder
)
select * from person