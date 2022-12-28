{% macro percent(value, total) %}
    round(cast({{ value }} as {{ elementary.elementary_type_float() }}) / nullif(cast({{ total }} as {{ elementary.elementary_type_float() }}), 0) * 100.0, 3)
{% endmacro %}