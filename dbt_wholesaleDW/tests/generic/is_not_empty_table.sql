{% test is_not_empty_table(model=model) %}
    with count_CTE as (
        select count(*) as count_num from {{ model }}
    )
    select * from count_CTE where count_num = 0
{% endtest %}