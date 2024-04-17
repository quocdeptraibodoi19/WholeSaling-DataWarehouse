{{ config(materialized='incremental') }}

with online_user_email_adress as (
    select
        t.bussinessentityid,
        s.emailaddress,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("ecomerce", "ecomerce_useremailaddress") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.userid = t.external_id and t.source = "ecom_user"
),
stakeholder_email_address as (
    select
        t.bussinessentityid,
        s.emailaddress,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_email") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.source = "StakeHolder" and t.source = "stakeholder" and s.id = t.external_id
),
employee_email_address as (
    select
        t.bussinessentityid,
        s.emailaddress,
        s.modifieddate,
        s.is_deleted,
        s.date_partition
    from {{ source("hr_system", "hr_system_email") }} s
    inner join {{ ref("person_BussinessEntity") }} t
    on s.source = "Employee" and t.source = "employee" and s.id = t.external_id
),
email_address as (
    select * from online_user_email_adress
    union all
    select * from stakeholder_email_address
    union all
    select * from employee_email_address
)
select 
    row_number() over(order by emailaddress) as emailaddressid,
    s.*
from email_address s
{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}
