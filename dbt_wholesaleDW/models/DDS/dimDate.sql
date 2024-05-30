{{ config(materialized='table') }}

with date_dimension as (
    {{ dbt_date.get_date_dimension("2011-01-01", "2014-12-31") }}
)
select 
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    *
From date_dimensionls
