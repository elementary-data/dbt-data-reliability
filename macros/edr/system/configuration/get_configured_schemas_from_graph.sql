{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% if execute %}
        {% set root_project = context["project_name"] %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes %}
            {% if node.resource_type in ['model', 'source', 'snapshot', 'seed'] and node.package_name == root_project %}
                {% set database_name = node.database %}
                {% set schema_name = node.schema %}
                {% if adapter.check_schema_exists(database_name, schema_name) %}
                    {% do configured_schemas.append((database_name, schema_name)) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(configured_schemas | unique | list ) }}
{% endmacro %}
