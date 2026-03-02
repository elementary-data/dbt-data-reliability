{% macro get_model_graph_node(model_relation) %}
    {% if execute %}
        {# model here is actually the test node in the graph #}
        {% set test_graph_node = model %}
        {% set test_depends_on_unique_ids = test_graph_node.depends_on.nodes %}
        {# model relation is the relation object of the model where the test is defined #}
        {% set relation_name = model_relation.name | lower %}
        {% set depends_on_nodes = elementary.get_nodes_by_unique_ids(test_depends_on_unique_ids) %}
        {% if depends_on_nodes %}
            {% for node in depends_on_nodes %}
                {% set node_name = node.name | lower %}
                {% set node_alias = node.get('alias', '') | lower %}
                {% set node_identifier = node.get('identifier', '') | lower %}
                {% if node_name == relation_name or node_alias == relation_name or node_identifier == relation_name %}
                    {{ return(node) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
