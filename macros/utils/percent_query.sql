{% macro percent(value, total) %}
    round(cast(cast({{ value }} as {{ elementary.type_float() }}) / nullif(cast({{ total }} as {{ elementary.type_float() }}), 0) * 100.0 as {{ elementary.type_numeric() }}), 3)
{% endmacro %}