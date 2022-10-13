{% macro get_elementary_relation(identifier) %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% do return(adapter.get_relation(elementary_database, elementary_schema, identifier)) %}
{% endmacro %}
