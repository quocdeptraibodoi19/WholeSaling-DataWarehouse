{{ config(materialized='incremental') }}

select
    positiontypeid as contacttypeid,
    positionname,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_stackholderposition") }}
