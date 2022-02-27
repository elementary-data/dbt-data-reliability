{% macro percent(value, total) %}
    round(cast({{ value }} as {{ dbt_utils.type_int() }}) /
    nullif(cast({{ total }} as {{ dbt_utils.type_int() }}), 0) * 100.0, 3)
{% endmacro %}