{% macro construct_metric_properties_dict(timestamp_column, where_expression, time_bucket) %}

  {%set d = {'timestamp_column': ( timestamp_column if timestamp_column else "Null" ) ,
             'where_expression': ( where_expression if where_expression else "Null" ) ,
             'time_bucket':      ( time_bucket if time_bucket else "Null" )            } %}
  {% do return(d) %}
{% endmacro %}