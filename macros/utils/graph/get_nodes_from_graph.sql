{% macro get_nodes_from_graph() %}
    {% set nodes = [] %}
    {% do nodes.extend(graph.sources.values()) %}
    {% do nodes.extend(graph.nodes.values()) %}
    {{ return(nodes) }}
{% endmacro %}
