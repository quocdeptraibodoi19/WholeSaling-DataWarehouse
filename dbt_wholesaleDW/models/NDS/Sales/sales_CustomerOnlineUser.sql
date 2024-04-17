{{ config(materialized='view') }}

with CTE as (
    select 
        bussinessentityid,
        external_id
    from {{ ref("person_BussinessEntity") }} 
    where source = "ecom_user"
)
select
    CTE.bussinessentityid as userid,
    s.userid as old_userid,
    s.accountnumber,
    s.modifieddate,
    s.is_deleted,
    s.date_partition
from {{ source("ecomerce", "ecomerce_user") }} s
inner join CTE
on CTE.external_id = s.userid