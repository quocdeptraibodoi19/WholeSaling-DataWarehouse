{{ config(materialized='incremental') }}

/*
    Basically, the creditcard now is the union between that in Wholesaling and Ecomerce (Simple Assumption). 
*/

with CTE as (
    select
        creditcardid as old_creditcardid,
        cardnumber,
        cardtype,
        expmonth,
        expyear,
        modifieddate,
        is_deleted,
        date_partition,
        "ecom" as source
    from {{ source('ecomerce', 'ecomerce_usercreditcard') }}
    union all
    select 
        creditcardid as old_creditcardid,
        cardnumber,
        cardtype,
        expmonth,
        expyear,
        modifieddate,
        is_deleted,
        date_partition,
        "wholesale" as source
    from {{ source('wholesale', 'wholesale_system_storerepcreditcard')}}
),
CTE_1 as (
    select 
        row_number() over(order by CTE.cardnumber, CTE.cardtype, CTE.expmonth, CTE.expyear) as creditcardid,
        CTE.*
    from CTE
)
select * from CTE_1
{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}