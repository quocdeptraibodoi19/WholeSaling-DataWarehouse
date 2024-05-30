{{ config(materialized='view') }}

select
    contacttypeid,
    `name`,
    modifieddate,
    is_deleted,
    extract_date
from {{ source("wholesale", "wholesale_system_contacttype") }}
