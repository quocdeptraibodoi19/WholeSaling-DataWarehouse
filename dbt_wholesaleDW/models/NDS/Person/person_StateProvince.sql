{{ config(materialized='view') }}

select
    stateprovinceid,
    stateprovincecode,
    countryregioncode,
    isonlystateprovinceflag,
    `name`,
    territoryid,
    modifieddate,
    is_deleted,
    date_partition
from {{"wholesale", "wholesale_system_stateprovince"}}