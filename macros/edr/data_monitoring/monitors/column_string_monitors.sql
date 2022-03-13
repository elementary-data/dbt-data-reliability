{% macro missing_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when trim({{ column_name }}, ' ') = '' then 1 when lower({{ column_name }}) = 'null' then 1 else 0 end), 0)
{% endmacro %}

{% macro missing_percent(column_name) %}
    {{ elementary.percent(elementary.missing_count(column_name), elementary.row_count()) }}
{% endmacro %}
