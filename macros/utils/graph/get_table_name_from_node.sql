{% macro get_table_name_from_node(node) %}
    {% if node.identifier %}
        {% set table_name = node.identifier %}
    {% elif node.alias %}
        {% set table_name = node.alias %}
    {% else %}
        {% set table_name = node.name %}
    {% endif %}
    {{ return(table_name) }}
{% endmacro %}