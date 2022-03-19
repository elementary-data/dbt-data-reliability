{% macro get_table_config_from_graph(model_relation, test_config) %}
    {% set table_config = {} %}
    {% set model_name = model_relation.name | lower %}
    {% set nodes = elementary.get_nodes_from_graph() %}
    {% for node in nodes %}
        {% set node_name = node.name | lower %}
        {% if node_name == model_name %}
            {% if node.unique_id in test_config.model.depends_on.nodes %}
                {% set model_config = node.get('config') %}
                {% if model_config and model_config is mapping %}
                    {% set elementary_config = model_config.get('elementary') %}
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
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ return(table_config) }}
{% endmacro %}