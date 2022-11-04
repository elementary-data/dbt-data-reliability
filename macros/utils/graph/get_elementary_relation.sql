{% macro get_elementary_relation(identifier) %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% if this and this.database == elementary_database and this.schema == elementary_schema and this.identifier == identifier %}
    {% do return(this) %}
  {% endif %}
  {% do return(adapter.get_relation(elementary_database, elementary_schema, identifier)) %}
{% endmacro %}
