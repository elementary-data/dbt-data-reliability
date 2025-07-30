{% macro is_elementary_enabled() %}
  {% if var('elementary_enabled', none) is not none %}
    {% do return(var('elementary_enabled')) %}
  {% endif %}
  {% do return("elementary" in graph) %}
{% endmacro %}
