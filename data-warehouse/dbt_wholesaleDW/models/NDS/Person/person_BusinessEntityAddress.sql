{{ config(materialized='view') }}

select
    s.bussiness_entity_id,
    t.addressid as address_id,
    t.address_type_id,
    t.valid_from,
    t.valid_to,
    t.is_deleted,
    t.is_valid,
    t.updated_at
from {{ ref("person_BussinessEntity") }} as s
inner join {{ ref("person_Address") }} t
on s.external_id = t.source_key and s.source = t.source