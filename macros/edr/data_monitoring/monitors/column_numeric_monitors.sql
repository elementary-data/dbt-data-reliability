{% macro zero_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when cast({{ column_name }} as {{ dbt_utils.type_int() }}) = 0 then 1 else 0 end), 0)
{% endmacro %}

{% macro zero_percent(column_name) %}
    {{ elementary.percent(elementary.zero_count(column_name), elementary.row_count()) }}
{% endmacro %}