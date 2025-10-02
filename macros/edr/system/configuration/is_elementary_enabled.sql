{% macro is_elementary_enabled() %}
  {% set database_name = elementary.get_package_database_and_schema()[0] %}
  {% do return(database_name is not none) %}
{% endmacro %}
