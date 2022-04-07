{% macro get_nodes_by_unique_ids(unique_ids) %}
    {% set nodes = []%}
    {% if execute %}
        {% if unique_ids and unique_ids is iterable %}
            {% for unique_id in unique_ids %}
                {% set node = elementary.get_node(unique_id) %}
                {% if node %}
                    {% do nodes.append(node) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(nodes) }}
{% endmacro %}