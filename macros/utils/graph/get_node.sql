{% macro get_node(node_unique_id) %}
    {# First let's try to find it in regular nodes #}
    {%- set node = graph.nodes.get(node_unique_id) -%}
    {%- if not node -%}
        {# If not found let's try to find it in source nodes #}
        {%- set node = graph.sources.get(node_unique_id) -%}
    {%- endif -%}
    {{ return(node) }}
{% endmacro %}