{% macro construct_metric_properties_dict(timestamp_column, where_expression, time_bucket) %}
  {%set d = {'timestamp_column': timestamp_column,
             'where_expression': where_expression,
             'time_bucket': time_bucket} %}
  {% do return(d) %}
{% endmacro %}