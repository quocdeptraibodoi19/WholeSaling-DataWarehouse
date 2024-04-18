{{ config(materialized='view') }}

select
    currencycode as CurrencyAlternateKey,
    `name` as CurrencyName
from {{ ref("sales_Currency") }}