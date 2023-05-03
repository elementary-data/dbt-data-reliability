{% test freshness_anomalies(model, timestamp_column, sensitivity, days_back, backfill_days, where_expression, time_bucket) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["freshness"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      days_back=days_back,
      backfill_days=backfill_days,
      where_expression=where_expression,
      time_bucket=time_bucket
    )
  }}
{% endtest %}
