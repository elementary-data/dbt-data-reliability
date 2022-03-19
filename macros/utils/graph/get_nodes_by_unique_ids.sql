{% macro get_nodes_by_unique_ids(unique_ids) %}
    {% set nodes = []%}
    {% if execute %}
        {% if unique_ids and unique_ids is iterable %}
            {% for unique_id in unique_ids %}
                {# First let's try to find it in regular nodes #}
                {%- set node = graph.nodes.get(unique_id) -%}
                {%- if not node -%}
                    {# If not found let's try to find it in source nodes #}
                    {%- set node = graph.sources.get(unique_id) -%}
                {%- endif -%}
                {% if node %}
                    {% do nodes.append(node) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(nodes) }}
{% endmacro %}