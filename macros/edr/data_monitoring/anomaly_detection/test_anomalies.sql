{% macro test_anomalies(bucketed_metrics_query, timestamp_column, days_back, where, sensitivity) %}
  {% do elementary.append_metrics(bucketed_metrics_query, timestamp_column, days_back, where) %}
{% endmacro %}
