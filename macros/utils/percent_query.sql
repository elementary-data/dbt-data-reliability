{% macro percent(value, total) %}
    {% set value = elementary.cast_as_float(value) %}
    {% set total = elementary.cast_as_float(total) %}
    {% set query %}
      round({{ elementary.cast_as_numeric('{} / nullif({}, 0) * 100.0'.format(value, total)) }}, 3)
    {% endset %}
    {% do return(query) %}
{% endmacro %}