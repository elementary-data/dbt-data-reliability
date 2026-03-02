{% macro null_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 else 0 end), 0)
{% endmacro %}

{% macro null_percent(column_name) %}
    {{ elementary.edr_percent(elementary.null_count(column_name), elementary.row_count()) }}
{% endmacro %}

{% macro not_null_percent(column_name) %}
    {{ elementary.edr_not_percent(elementary.null_count(column_name), elementary.row_count()) }}
{% endmacro %}
