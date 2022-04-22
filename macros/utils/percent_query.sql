{% macro percent(value, total) %}
    round(cast({{ value }} as {{ dbt_utils.type_numeric() }}) / nullif(cast({{ total }} as {{ dbt_utils.type_numeric() }}), 0) * 100.0, 3)
{% endmacro %}