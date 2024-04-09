{% macro get_available_monitors() %}
    {% do return({
      'table': ['row_count', 'freshness'],
      'column_any_type': ['null_count', 'null_percent', 'not_null_percent'],
      'column_string': ['min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent', 'not_missing_percent'],
      'column_numeric': ['min', 'max', 'zero_count', 'zero_percent', 'not_zero_percent', 'average', 'standard_deviation', 'variance', 'sum'],
      'column_boolean': ['count_true', 'count_false']
    }) %}
{% endmacro %}

{% macro get_default_monitors() %}
    {% do return({
      'table': ['row_count', 'freshness'],
      'column_any_type': ['null_count', 'null_percent'],
      'column_string': ['min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent'],
      'column_numeric': ['min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance'],
      'column_boolean': ['count_true', 'count_false']
    }) %}
{% endmacro %}
