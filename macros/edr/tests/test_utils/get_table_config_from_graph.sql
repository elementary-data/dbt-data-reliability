{% macro get_table_config_from_graph(model_relation) %}
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
                        {% set timestamp_column = elementary_config.get('timestamp_column') %}
                        {% if timestamp_column %}
                            {% set columns_from_relation = adapter.get_columns_in_relation(model_relation) %}
                            {% if columns_from_relation and columns_from_relation is iterable %}
                                {% for column_obj in columns_from_relation %}
                                    {% if column_obj.column | lower == timestamp_column | lower %}
                                        {% set timestamp_column_data_type = elementary.normalize_data_type(column_obj.dtype) %}
                                        {% do table_config.update({'timestamp_column': timestamp_column,
                                                                   'timestamp_column_data_type': timestamp_column_data_type}) %}
                                    {% endif %}
                                {% endfor %}
                            {% endif %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
    {{ return(table_config) }}
{% endmacro %}