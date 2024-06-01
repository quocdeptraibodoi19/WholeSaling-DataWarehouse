{{ config(materialized='view') }}

with CTE as (
    select 
        bussiness_entity_id,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = '{{ env_var("ecom_source") }}_user'
)
select
    CTE.bussiness_entity_id as user_id,
    s.user_id as old_userid,
    s.account_number,
    s.extract_date,
    s.updated_at,
    s.valid_from,
    s.valid_to,
    s.is_deleted,
    s.is_valid
from {{ ref("stg__ecomerce_user") }} s
inner join CTE
on CTE.external_id = s.user_id