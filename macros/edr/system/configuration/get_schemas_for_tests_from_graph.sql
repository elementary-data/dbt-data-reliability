{% macro get_schemas_for_tests_from_graph() %}
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
                            {% if adapter.check_schema_exists(node['database'], node['schema']) %}
                                {% do configured_schemas.append((node['database'], node['schema'])) %}
                            {% endif %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(configured_schemas | unique | list ) }}
{% endmacro %}


{% macro get_schemas_for_tests_from_graph_as_tuple() %}

    {%- set tests_schemas_tuples = elementary.get_schemas_for_tests_from_graph() %}
    {%- set schemas_list = [] %}

    {%- for tests_schemas_tuple in tests_schemas_tuples %}
        {%- set database_name, schema_name = tests_schemas_tuple %}
        {%- set full_schema_name = database_name | upper ~ '.' ~ schema_name | upper %}
        {%- do schemas_list.append(full_schema_name) -%}
    {%- endfor %}

    {% set schemas_tuple = elementary.strings_list_to_tuple(schemas_list) %}
    {{ return(schemas_tuple) }}

{% endmacro %}