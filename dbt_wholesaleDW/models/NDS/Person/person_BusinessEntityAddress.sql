{{ config(materialized='view') }}

select
    s.bussinessentityid,
    t.addressid,
    t.addresstypeid,
    t.modifieddate
from {{ ref("person_BussinessEntity") }} as s
inner join {{ ref("person_Address") }} t
on s.external_id = t.source_key and s.source = t.source