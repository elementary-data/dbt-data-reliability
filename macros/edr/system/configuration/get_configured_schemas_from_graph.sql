{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% set existing_schemas = [] %}
    {% if execute %}
        {% set root_project = context["project_name"] %}
        {{ print("============================================") }}
        {{ print("root_project: " ~ root_project) }}
        {{ print("============================================") }}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes %}
            {% if node.resource_type in ['model', 'source', 'snapshot', 'seed'] and node.package_name == root_project %}
                {{ print("node.package_name: " ~ toyaml(node)) }}
                {{ print("condition1: " ~ node.package_name == root_project) }}
                {{ print("condition2: " ~ node.resource_type in ['model', 'source', 'snapshot', 'seed'] and node.package_name == root_project ) }}
                {% set schema_tuple = (node.database, node.schema) %}
                {% if schema_tuple not in configured_schemas %}
                    {{ print("node.resource_type: " ~ node.resource_type) }}
                    {{ print("node.pacakge_name:: " ~ node.package_name) }}
                    {{ print("schema_tuple: " ~ toyaml(schema_tuple)) }}
                    {% do configured_schemas.append(schema_tuple) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {% for schema_tuple in configured_schemas %}
            {% set database_name = schema_tuple[0] %}
            {% set schema_name = schema_tuple[1] %}
            {% if adapter.check_schema_exists(database_name, schema_name) %}
                {% do existing_schemas.append(schema_tuple) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(existing_schemas) }}
{% endmacro %}
