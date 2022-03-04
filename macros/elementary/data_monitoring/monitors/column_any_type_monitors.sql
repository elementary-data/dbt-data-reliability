{% macro null_count_monitor(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 else 0 end), 0)
{% endmacro %}

{% macro null_percent_monitor(column_name) %}
    {{ elementary.percent(elementary.null_count_monitor(column_name), row_count_monitor()) }}
{% endmacro %}