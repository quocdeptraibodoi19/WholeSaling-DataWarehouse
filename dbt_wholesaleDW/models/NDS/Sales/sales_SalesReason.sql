{{ config(materialized='view') }}

Select count(*) from {{ source('ecomerce', 'ecomerce_salesreason') }}  
