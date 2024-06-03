{{ config(materialized='view') }}

select
    stackholderid as stackholder_id,
    persontype as person_type,
    namestyle as name_style,
    title,
    firstname as first_name,
    middlename as middle_name,
    lastname as last_name,
    suffix as suffix,
    emailpromotion as email_promotion,
    additionalcontactinfo as additional_contact_info,
    demographics,
    birthdate,
    maritalstatus as marital_status,
    gender,
    totalchildren as total_children,
    numberchildrenathome as number_children_at_home,
    houseownerflag as house_owner_flag,
    numbercarsowned as number_cars_owned,
    datefirstpurchase as date_first_purchase,
    commutedistance as commuted_distance,
    education,
    occupation,
    extract_date,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    is_deleted,
    case
        when is_deleted = 'True' or dbt_valid_to != 'NULL' then 0
        else 1
    end as is_valid
from {{ ref("hr_system_stakeholder_snapshot") }}