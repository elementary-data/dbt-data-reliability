{% macro get_node_by_name(name) %}
    {%- set nodes = get_nodes_from_graph() -%}
    {% for node in nodes %}
        {% if node.name == name %}
            {% do return(node) %}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}
