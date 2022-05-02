{% macro get_test_execution_id() %}
    {{ return(elementary.get_node_execution_id(model)) }}
{% endmacro %}