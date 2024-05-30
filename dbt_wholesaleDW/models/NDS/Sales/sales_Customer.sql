{{ config(materialized='incremental') }}

with CTE as ( 
    select
        userid as personid,
        null as storeid,
        accountnumber,
        modifieddate,
        is_deleted,
        extract_date
    from {{ ref("sales_CustomerOnlineUser") }} 
    union all
    select 
        storerepid as personid,
        storeid,
        accountnumber,
        modifieddate,
        is_deleted,
        extract_date
    from {{ ref("sales_CustomerStoreUser") }}
),
CTE_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['personid', 'storeid', 'accountnumber']) }} as customerid,    
        CTE.*
    from CTE
)
select * from CTE_1
{% if is_incremental() %}

    where modifieddate >= ( select max(modifieddate) from {{ this }} )

{% endif %}
