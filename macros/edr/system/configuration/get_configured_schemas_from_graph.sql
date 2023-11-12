{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% set existing_schemas = [] %}
    {% if execute %}
        {% set root_project = context["project_name"] %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes %}
            {% if node.resource_type in ['model', 'source', 'snapshot', 'seed'] and node.package_name == root_project %}
                {% set schema_tuple = (node.database, node.schema) %}
                {% if schema_tuple not in configured_schemas %}
                    {% do configured_schemas.append(schema_tuple) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {% for schema_tuple in configured_schemas %}
            {% set database_name = schema_tuple[0] %}
            {% set schema_name = schema_tuple[1] %}
            {% if elementary.schema_exists(database_name, schema_name) %}
                {% do existing_schemas.append(schema_tuple) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(existing_schemas) }}
{% endmacro %}
