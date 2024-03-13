{{ config(materialized='view') }}

/*
    Basically, the creditcard now is the union between that in Wholesaling and Ecomerce (Simple Assumption). 
*/

with CTE as (
    select
        row_number() over(order by cardnumber, cardtype, expmonth, expyear, is_deleted) as creditcardid,
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
        row_number() over(order by cardnumber, cardtype, expmonth, expyear, is_deleted) as creditcardid,
        cardnumber,
        cardtype,
        expmonth,
        expyear,
        modifieddate,
        is_deleted,
        date_partition
    from {{ source('wholesale', 'wholesale_system_storerepcreditcard') }}
)
select * from CTE