{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% set existing_schemas = [] %}
    {% if execute %}
        {% set root_project = context["project_name"] %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes %}
            {% if node.resource_type in ['model', 'source', 'snapshot', 'seed'] and node.package_name == root_project %}
                {% set database_name = node.database %}
                {% set schema_name = node.schema %}
                {% do configured_schemas.append((database_name, schema_name)) %}
            {% endif %}
        {% endfor %}
        {%- set configured_schemas = configured_schemas | unique | list %}
        {%- for schema_tupple in configured_schemas %}
            {% set database_name = schema_tupple[0] %}
            {% set schema_name = schema_tupple[1] %}
            {% if adapter.check_schema_exists(database_name, schema_name) %}
                {% do existing_schemas.append((database_name, schema_name)) %}
            {% endif %}
        {%- endfor %}
    {% endif %}
    {{ return(existing_schemas | unique | list ) }}
{% endmacro %}
