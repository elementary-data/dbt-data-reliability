{% macro is_on_run_end() %}
  {% do return('on-run-end' in model.tags) %}
{% endmacro %}
