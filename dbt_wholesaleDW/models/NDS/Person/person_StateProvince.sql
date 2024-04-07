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
from {{ source("wholesale", "wholesale_system_stateprovince") }}