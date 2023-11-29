{% macro get_configured_databases_from_graph() %}
    {% set schema_tuples = elementary.get_configured_schemas_from_graph() %}
    {% do return(schema_tuples | map(attribute=0) | unique) %}
{% endmacro %}