{{ config(materialized='incremental') }}

with CTE as ( 
    select
        user_id as person_id,
        null as store_id,
        account_number,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("sales_CustomerOnlineUser") }} 
    union all
    select 
        store_rep_id as person_id,
        store_id,
        account_number,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid
    from {{ ref("sales_CustomerStoreUser") }}
),
CTE_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['person_id', 'store_id', 'account_number']) }} as customer_id,    
        CTE.*
    from CTE
)
select * from CTE_1
where 1 = 1
{% if is_incremental() %}

    and updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}
