{% macro get_metric_properties(
  model_graph_node,
  timestamp_column,
  where_expression,
  time_bucket,
  dimensions=none,
  freshness_column=none,
  event_timestamp_column=none,
  collected_by=none
) %}
    {% set timestamp_column = elementary.get_test_argument('timestamp_column', timestamp_column, model_graph_node) %}
    {% set where_expression = elementary.get_test_argument('where_expression', where_expression, model_graph_node) %}
    {% set time_bucket = elementary.get_time_bucket(time_bucket, model_graph_node) %}
    {% set freshness_column = elementary.get_test_argument('freshness_column', freshness_column, model_graph_node) %}
    {% set event_timestamp_column = elementary.get_test_argument('event_timestamp_column', event_timestamp_column, model_graph_node) %}
    {% set dimensions = elementary.get_test_argument('dimensions', dimensions, model_graph_node) %}
    {% set metric_props = {
      'timestamp_column': timestamp_column,
      'where_expression': where_expression,
      'time_bucket': time_bucket,
      'freshness_column': freshness_column,
      'event_timestamp_column': event_timestamp_column,
      'dimensions': dimensions
    } %}
    {% if collected_by %}
      {% do metric_props.update({'collected_by': collected_by}) %}
    {% endif %}
    {% do return(metric_props) %}
{% endmacro %}
