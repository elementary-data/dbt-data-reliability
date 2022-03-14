{% macro no_results_query() %}
    with nothing as (select 1 as num)
    select * from nothing where num = 2
{% endmacro %}