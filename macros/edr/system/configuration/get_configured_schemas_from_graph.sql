{% macro get_configured_schemas_from_graph() %}
    {% set configured_schemas = [] %}
    {% if execute %}
        {% for test_node in graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
            {% set test_metadata = test_node.get('test_metadata') %}
            {% if test_metadata %}
                {% set test_name = test_metadata.get('name') %}
                {% if test_name == 'schema_changes' %}
                    {% set test_depends_on_unique_ids = test_node.depends_on.nodes %}
                    {% set depends_on_nodes = elementary.get_nodes_by_unique_ids(test_depends_on_unique_ids) %}
                    {% for node in depends_on_nodes %}
                        {% set node_package_name = node.get('package_name') %}
                        {% if node_package_name != 'elementary' %}
                            {% if adapter.check_schema_exists(node.database, node.schema) %}
                                    {% set schema_relation = api.Relation.create(database=node.database, schema=node.schema).without_identifier() %}
                                    {% if schema_relation %}
                                        {% set full_schema_name = schema_relation.render() %}
                                        {% do configured_schemas.append(full_schema_name | upper) %}
                                    {% endif %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(configured_schemas | unique | list ) }}
{% endmacro %}
