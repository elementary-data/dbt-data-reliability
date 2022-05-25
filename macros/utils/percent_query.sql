{% macro percent(value, total) %}
    round(cast({{ value }} as {{ dbt_utils.type_float() }}) / nullif(cast({{ total }} as {{ dbt_utils.type_float() }}), 0) * 100.0, 3)
{% endmacro %}