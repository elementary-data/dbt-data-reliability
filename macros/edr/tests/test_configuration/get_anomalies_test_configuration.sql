{% macro get_anomalies_test_configuration(model_relation, timestamp_column, where_expression, time_bucket) %}
    {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}

    {%- set timestamp_column = elementary.get_timestamp_column(timestamp_column, model_graph_node, model_relation) %}
    {%- set time_bucket = elementary.get_time_bucket(time_bucket, model_graph_node) %}
    {%- set where_expression = elementary.get_test_argument('where_expression', where_expression, model_graph_node) %}

    {{ return([timestamp_column, time_bucket, where_expression]) }}
{% endmacro %}


