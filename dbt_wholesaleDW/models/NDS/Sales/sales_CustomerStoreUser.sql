{{ config(materialized='view') }}

with CTE as(
    select 
        bussiness_entity_id,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = '{{ env_var("hr_source") }}_stakeholder'
),
CTE2 as (
    select 
        bussiness_entity_id,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = '{{ env_var("wholesale_source") }}_store'
)
select
    s.customer_id as old_store_customer_id,
    CTE2.bussiness_entity_id as store_rep_id,
    s.store_rep_id as old_store_rep_id,
    CTE.bussiness_entity_id as store_id,
    s.store_id as old_store_id,
    s.account_number,
    s.extract_date,
    s.updated_at,
    s.valid_from,
    s.valid_to,
    s.is_deleted,
    s.is_valid
from {{ ref("stg__wholesale_system_storecustomer") }} s
inner join CTE
on CTE.external_id = s.store_id
left join CTE2
on CTE2.external_id = s.store_rep_id