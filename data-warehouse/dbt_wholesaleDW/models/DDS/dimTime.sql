{{ config(materialized='table') }}

WITH time_dimension AS (
    {{ generate_time_dimension() }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['time_of_day']) }} AS time_key,
    time_of_day,
    hour_of_day,
    minute_of_hour,
    second_of_minute
FROM time_dimension
