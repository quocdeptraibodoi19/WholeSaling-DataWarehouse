{% macro generate_time_dimension() %}
    SELECT 
        CONCAT(
            LPAD(CAST(hours AS STRING), 2, '0'), ':', 
            LPAD(CAST(minutes AS STRING), 2, '0'), ':', 
            LPAD(CAST(seconds AS STRING), 2, '0')
        ) AS time_of_day,
        hours AS hour_of_day,
        minutes AS minute_of_hour,
        seconds AS second_of_minute
    FROM (
        SELECT
            hours,
            minutes,
            seconds
        FROM
            (SELECT explode(sequence(0, 23)) AS hours) hours,
            (SELECT explode(sequence(0, 59)) AS minutes) minutes,
            (SELECT explode(sequence(0, 59)) AS seconds) seconds
    ) time
{% endmacro %}
