{% macro get_node_execution_id(node) %}
    {% set node_execution_id = [invocation_id, node.get('unique_id')] | join('.') %}
    {{ return(node_execution_id) }}
{% endmacro %}