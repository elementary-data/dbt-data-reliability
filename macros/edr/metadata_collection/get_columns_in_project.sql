{% macro get_columns_in_project() %}
    {% set configured_schemas = elementary.get_configured_schemas_from_graph() %}
    {{ elementary.get_columns_by_schemas(configured_schemas) }}
{% endmacro %}
