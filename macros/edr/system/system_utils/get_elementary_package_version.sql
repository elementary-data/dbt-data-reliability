{% macro get_elementary_package_version() %}
  {% set conf = elementary.get_runtime_config() %}
  {% do return(conf.dependencies["elementary"].version) %}
{% endmacro %}
