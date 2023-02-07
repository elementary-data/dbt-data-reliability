{% macro result_value(single_column_query) %}
    {% set result = elementary.run_query(single_column_query) %}
    {% if not result %}
      {% do return(none) %}
    {% endif %}
    {% do return(result[0][0]) %}
{% endmacro %}
