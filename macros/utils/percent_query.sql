{% macro edr_percent(value, total) %}
    {% set value = elementary.edr_cast_as_float(value) %}
    {% set total = elementary.edr_cast_as_float(total) %}
    {% set query %}
      round({{ elementary.edr_cast_as_numeric('{} / nullif({}, 0) * 100.0'.format(value, total)) }}, 3)
    {% endset %}
    {% do return(query) %}
{% endmacro %}

{% macro edr_not_percent(value, total) %}
    {% set value = elementary.edr_cast_as_float(value) %}
    {% set total = elementary.edr_cast_as_float(total) %}
    {% set query %}
      round({{ elementary.edr_cast_as_numeric('100 - ({} / nullif({}, 0) * 100.0)'.format(value, total)) }}, 3)
    {% endset %}
    {% do return(query) %}
{% endmacro %}