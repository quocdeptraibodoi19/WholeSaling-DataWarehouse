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
        extract_date,
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
        extract_date,
        "wholesale" as source
    from {{ source('wholesale', 'wholesale_system_storerepcreditcard')}}
),
CTE_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['cardnumber', 'cardtype', 'expmonth', 'expyear']) }} as creditcardid,
        CTE.*
    from CTE
)
select * from CTE_1
{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}