{% macro get_model_database_and_schema_from_test_node(test_node) %}
    {% set test_database = test_node.get('database') %}
    {% set test_schema = test_node.get('schema') %}
    {% set config_dict = elementary.safe_get_with_default(test_node, 'config', {}) %}
    {% set test_schema_sufix = config_dict.get('schema') %}
    {% if test_schema and test_schema_sufix %}
        {% set test_schema = test_schema | replace('_' ~ test_schema_sufix, '') %}
    {% endif %}
    {{ return([test_database, test_schema]) }}
{% endmacro %}