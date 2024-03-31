{{ config(materialized='incremental') }}

select
    contacttypeid,
    `name`,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("wholesale", "wholesale_system_contacttype") }}
