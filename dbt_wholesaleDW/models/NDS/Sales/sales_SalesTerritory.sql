{{ config(materialized='view') }}

select
    territoryid,
    `name`,
    countryregioncode,
    `group`,
    salesytd,
    saleslastyear,
    costlastyear,
    costytd,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("hr_system", "hr_system_salesterritory") }}