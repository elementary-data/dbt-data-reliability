{% macro is_elementary_enabled() %}
  {% do return("elementary" in graph) %}
{% endmacro %}
