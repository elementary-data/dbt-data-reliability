{% macro get_runtime_config() %}
  {{ return(builtins.ref.config) }}
{% endmacro %}
