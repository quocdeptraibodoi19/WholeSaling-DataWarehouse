{{ config(materialized='view') }}

select
    employeeid as employee_id,
    nationalidnumber as national_id_number,
    loginid as login_id,
    organizationnode as organization_node,
    organizationlevel as organization_level,
    jobtitle as job_title,
    birthdate,
    maritalstatus as marital_status,
    gender,
    hiredate,
    salariedflag as salaried_flag,
    vacationhours as vacation_hours,
    sickleavehours as sick_leave_hours,
    currentflag as current_flag,
    persontype as person_type,
    namestyle as name_style,
    title,
    firstname as first_name,
    middlename as middle_name,
    lastname as last_name,
    suffix,
    emailpromotion as email_promotion,
    additionalcontactinfo as additional_contact_info,
    demographics,
    totalchildren as total_children,  
    numberchildrenathome as number_children_at_home,
    houseownerflag as house_owner_flag,
    numbercarsowned as number_cars_owned,
    datefirstpurchase as date_first_purchase,
    commutedistance as commuted_distance,
    education,
    occupation,
    passwordhash,
    passwordsalt,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("hr_system_employee_snapshot") }}