{{ config(materialized='view') }}

/*
    Basically, the creditcard now is the union between that in Wholesaling and Ecomerce (Simple Assumption). 
*/

with CTE as (
    select
        cardnumber,
        cardtype,
        expmonth,
        expyear,
        modifieddate,
        is_deleted,
        date_partition
    from {{ source('ecomerce', 'ecomerce_usercreditcard') }}
    union all
    select
        cardnumber,
        cardtype,
        expmonth,
        expyear,
        modifieddate,
        is_deleted,
        date_partition
    from {{ source('wholesale', 'wholesale_system_storerepcreditcard') }}
)
select 
    row_number() over(order by CTE.cardnumber, CTE.cardtype, CTE.expmonth, CTE.expyear) as creditcardid,
    CTE.*
from CTE