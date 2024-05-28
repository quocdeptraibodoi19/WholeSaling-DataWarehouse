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
    extract_date
from {{ source("wholesale", "wholesale_system_stateprovince") }}