{% macro get_timestamp_column_from_graph(model_relation) %}
    {% set table_config = {} %}
    {% if execute %}
        {# model here is actually the test node in the graph #}
        {% set test_depends_on_unique_ids = model.depends_on.nodes %}
        {# model relation is the relation object of the model where the test is defined #}
        {% set model_name = model_relation.name | lower %}
        {% set depends_on_nodes = elementary.get_nodes_by_unique_ids(test_depends_on_unique_ids) %}
        {% if depends_on_nodes %}
            {% for node in depends_on_nodes %}
                {% if node.name | lower == model_name %}
                    {% set elementary_config = elementary.get_elementary_config_from_node(node) %}
                    {% if elementary_config and elementary_config is mapping %}
                        {{ return(elementary_config.get('timestamp_column')) }}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}
