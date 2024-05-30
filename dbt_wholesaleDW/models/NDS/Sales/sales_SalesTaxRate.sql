{{ config(materialized='view') }}

/*
    This assumes that SalesTaxRate tables are the same in all sources, which is not the case in practice.
    However, this helps for the simplicity of the data warehouse. 
*/

-- We can still use: SELECT * FROM {{ source('wholesale', 'wholesale_system_salestaxrate') }}
select 
    salestaxrateid,
    stateprovinceid,
    taxtype,
    taxrate,
    name,
    modifieddate,
    is_deleted,
    extract_date
from {{ source('ecomerce', 'ecomerce_salestaxrate') }}
