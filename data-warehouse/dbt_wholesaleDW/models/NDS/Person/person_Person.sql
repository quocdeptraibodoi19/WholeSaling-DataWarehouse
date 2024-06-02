{{ config(materialized='view') }}

with valid_bussiness_entity as (
    select * from {{ ref("person_BussinessEntity") }}
    where is_valid = 1
),
online_user as (
    select
        t.bussiness_entity_id,
        "IN" as person_type,
        s.name_style,
        s.title,
        s.first_name,
        s.middle_name,
        s.last_name,
        s.suffix,
        s.email_promotion,
        s.additional_contact_info,
        s.demographics,
        s.birthdate,
        s.marital_status,
        s.gender,
        s.total_children,
        s.number_children_at_home,
        s.house_owner_flag,
        s.number_cars_owned,
        s.date_first_purchase,
        s.commuted_distance,
        s.education,
        s.occupation,
        t.extract_date,
        t.updated_at,
        t.valid_from,
        t.valid_to,
        t.is_deleted,
        t.is_valid
    from {{ ref("stg__ecomerce_user") }} s
    inner join valid_bussiness_entity t
    on s.user_id = t.external_id and t.source = '{{ env_var("ecom_source") }}_user'
),
employee as (
    select
        t.bussiness_entity_id,
        "EM" as person_type,
        s.name_style,
        s.title,
        s.first_name,
        s.middle_name,
        s.last_name,
        s.suffix,
        s.email_promotion,
        s.additional_contact_info,
        s.demographics,
        s.birthdate,
        s.marital_status,
        s.gender,
        s.total_children,
        s.number_children_at_home,
        s.house_owner_flag,
        s.number_cars_owned,
        s.date_first_purchase,
        s.commuted_distance,
        s.education,
        s.occupation,
        t.extract_date,
        t.updated_at,
        t.valid_from,
        t.valid_to,
        t.is_deleted,
        t.is_valid
    from {{ ref("stg__hr_system_employee") }} s
    inner join valid_bussiness_entity t
    on s.employee_id = t.external_id and t.source = '{{ env_var("hr_source") }}_employee'
),
stackeholder as (
    select
        t.bussiness_entity_id,
        "SC" as person_type,
        s.name_style,
        s.title,
        s.first_name,
        s.middle_name,
        s.last_name,
        s.suffix,
        s.email_promotion,
        s.additional_contact_info,
        s.demographics,
        s.birthdate,
        s.marital_status,
        s.gender,
        s.total_children,
        s.number_children_at_home,
        s.house_owner_flag,
        s.number_cars_owned,
        s.date_first_purchase,
        s.commuted_distance,
        s.education,
        s.occupation,
        t.extract_date,
        t.updated_at,
        t.valid_from,
        t.valid_to,
        t.is_deleted,
        t.is_valid
    from {{ ref("stg__hr_system_stakeholder") }} s
    inner join valid_bussiness_entity t
    on s.stackholder_id = t.external_id and t.source = '{{ env_var("hr_source") }}_stakeholder'
),
person as (
    select * from online_user
    union all
    select * from employee
    union all
    select * from stackeholder
)
select * from person