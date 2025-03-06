{% macro get_node_by_name(name, resource_type=none) %}
    {%- set nodes = elementary.get_nodes_from_graph() -%}
    {% for node in nodes %}
        {% if node.name == name and (resource_type is none or node.resource_type == resource_type) %}
            {% do return(node) %}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}
