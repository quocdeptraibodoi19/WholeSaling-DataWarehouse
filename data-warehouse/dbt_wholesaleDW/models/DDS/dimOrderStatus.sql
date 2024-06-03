{{ config(materialized='table') }}

with order_statuses as (
  select
    1 as order_status_id,
    'In process' as order_status_name
  union all
  select
    2 as order_status_id,
    'Approved' as order_status_name
  union all
  select
    3 as order_status_id,
    'Backordered' as order_status_name
  union all
  select
    4 as order_status_id,
    'Rejected' as order_status_name
  union all
  select
    5 as order_status_id,
    'Shipped' as order_status_name
  union all
  select
    6 as order_status_id,
    'Cancelled' as order_status_name
)

select 
  *,
  {{ dbt_utils.generate_surrogate_key(['order_status_id']) }} as order_status_key
from order_statuses