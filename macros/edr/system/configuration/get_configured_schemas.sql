{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% if execute %}
        {% set nodes = elementary.get_nodes_from_graph() %}
        {% for node in nodes | selectattr('resource_type', 'in', 'seed,source,model') %}
            {% if 'config' in node %}
                {% set node_config = node.config %}
                {% if node_config and node_config is mapping %}
                    {% if 'elementary' in node_config %}
                        {% set schema_relation = api.Relation.create(database=node.database, schema=node.schema).without_identifier() %}
                        {% if schema_relation %}
                            {% set full_schema_name = schema_relation.render() %}
                            {% do configured_schemas.append(full_schema_name) %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(configured_schemas | unique | list ) }}
{% endmacro %}
