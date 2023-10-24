{% macro get_elementary_relation(identifier) %}
  {%- if execute %}
    {%- set identifier_node = elementary.get_node('model.elementary.' ~ identifier) %}
    {%- if identifier_node -%}
      {%- set identifier_alias = elementary.safe_get_with_default(identifier_node, 'alias', identifier) %}
    {%- else -%}
      {% set identifier_alias = identifier %}
    {%- endif -%}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% if this and this.database == elementary_database and this.schema == elementary_schema and this.identifier == identifier_alias %}
      {% do return(this) %}
    {% endif %}
    {% do return(adapter.get_relation(elementary_database, elementary_schema, identifier_alias)) %}
  {%- endif %}
{% endmacro %}
