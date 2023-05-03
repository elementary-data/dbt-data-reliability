{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% if execute %}
        {% set root_project = context["project_name"] %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes %}
            {% if node["package_name"] == root_project %}
                {% if adapter.check_schema_exists(node['database'], node['schema']) %}
                    {% do configured_schemas.append((node['database'], node['schema'])) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(configured_schemas | unique | list ) }}
{% endmacro %}
