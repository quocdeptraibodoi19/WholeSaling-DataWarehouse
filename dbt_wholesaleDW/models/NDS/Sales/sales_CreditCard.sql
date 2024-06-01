{{ config(materialized='incremental') }}

/*
    Basically, the creditcard now is the union between that in Wholesaling and Ecomerce (Simple Assumption). 
*/

with CTE as (
    select
        credit_card_id as old_credit_card_id,
        card_number,
        card_type,
        exp_month,
        exp_year,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("ecom_source") }}_user' as source
    from {{ ref('stg__ecomerce_usercreditcard') }}
    union all
    select 
        credit_card_id as old_credit_card_id,
        card_number,
        card_type,
        exp_month,
        exp_year,
        extract_date,
        updated_at,
        valid_from,
        valid_to,
        is_deleted,
        is_valid,
        '{{ env_var("wholesale_source") }}_store' as source
    from {{ ref('stg__wholesale_system_storerepcreditcard') }}
),
CTE_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['card_number', 'card_type', 'exp_month', 'exp_year']) }} as credit_card_id,
        CTE.*
    from CTE
)
select * from CTE_1
{% if is_incremental() %}

    where updated_at >= ( select max(updated_at) from {{ this }} )

{% endif %}