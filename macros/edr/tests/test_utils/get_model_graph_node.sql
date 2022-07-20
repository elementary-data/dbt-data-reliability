{% macro get_model_graph_node(model_relation) %}
    {% if execute %}
        {# model here is actually the test node in the graph #}
        {% set test_graph_node = model %}
        {% set test_depends_on_unique_ids = test_graph_node.depends_on.nodes %}
        {# model relation is the relation object of the model where the test is defined #}
        {% set model_node = elementary.get_node_by_relation(model_relation) %}
        {% set depends_on_nodes = elementary.get_nodes_by_unique_ids(test_depends_on_unique_ids) %}
        {% if depends_on_nodes %}
            {% for node in depends_on_nodes %}
                {% if node.unique_id == model_node.unique_id %}
                    {{ return(node) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
