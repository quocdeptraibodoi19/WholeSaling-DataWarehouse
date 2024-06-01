{{ config(materialized='view') }}

-- Using mapping to map to the BussinessEntityId
with CTE as(
    select 
        bussiness_entity_id,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = '{{ env_var("hr_source") }}_employee'
)
select
    CTE.bussiness_entity_id,
    employee_id as old_employee_id,
    national_id_number,
    login_id,
    organization_node,
    organization_level,
    job_title,
    birthdate,
    marital_status,
    gender,
    hiredate,
    salaried_flag,
    vacation_hours,
    sick_leave_hours,
    current_flag,
    person_type,
    name_style,
    title,
    first_name,
    middle_name,
    last_name,
    suffix,
    email_promotion,
    additional_contact_info,
    demographics,
    total_children,
    number_children_at_home,
    house_owner_flag,
    number_cars_owned,
    date_first_purchase,
    commuted_distance,
    education,
    occupation,
    passwordhash,
    passwordsalt,
    extract_date,
    updated_at,
    valid_from,
    valid_to,
    is_deleted,
    is_valid
from {{ ref("stg__hr_system_employee") }}
inner join CTE
on CTE.external_id = employee_id