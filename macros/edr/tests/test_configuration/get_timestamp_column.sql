{% macro get_timestamp_column(timestamp_column, model_graph_node, model_relation) %}
    {%- set timestamp_column = elementary.get_test_argument('timestamp_column', timestamp_column, model_graph_node) %}
    {%- if timestamp_column %}
        {%- set is_timestamp = elementary.get_is_timestamp(timestamp_column, model_relation) %}
        {%- if is_timestamp %}
            {{ return(timestamp_column) }}
        {%- endif %}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_is_timestamp(timestamp_column, model_relation) %}
    {%- set timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model_relation, timestamp_column) %}
    {{ elementary.debug_log('timestamp_column - ' ~ timestamp_column) }}
    {{ elementary.debug_log('timestamp_column_data_type - ' ~ timestamp_column) }}
    {%- set is_timestamp = elementary.get_is_column_timestamp(model_relation, timestamp_column, timestamp_column_data_type) %}
    {{ elementary.debug_log('is_timestamp - ' ~ is_timestamp) }}

    {% if not is_timestamp %}
      {% do exceptions.raise_compiler_error("Column `{}` is not a timestamp.".format(metric_properties.timestamp_column)) %}
    {% endif %}
    {{ return(is_timestamp) }}
{% endmacro %}