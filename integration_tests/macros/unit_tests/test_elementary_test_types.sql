{% macro test_elementary_test_types() %}
    {# assert that all the elementary tests are identified as elementary types #}
    {%- set expected_elementary_test_types = ['anomaly_detection', 'schema_change'] %}
    {%- set elementary_test_types = [] %}
    {% for test_node in graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% set test_metadata = test_node.get('test_metadata') %}
        {% if test_metadata %}
            {% set test_name = test_metadata.get('name') %}
            {% set package_name = test_metadata.get('namespace') %}
                {% if package_name == 'elementary' %}
                    {%- set flattened_test_mock = {"test_namespace" : "elementary", "short_name" : test_name}%}
                    {%- set test_type = elementary.get_elementary_test_type(flattened_test_mock) %}
                    {%- do elementary_test_types.append(test_type) -%}
                {% endif %}
        {% endif %}
    {% endfor %}
    {%- set elementary_test_types = elementary_test_types | unique | list %}
    {{ assert_lists_contain_same_items(elementary_test_types, expected_elementary_test_types ) }}
{% endmacro %}