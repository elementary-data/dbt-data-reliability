{% test volume_anomalies(model, timestamp_column, sensitivity, days_back, backfill_days, where_expression, time_bucket, anomaly_direction='both', seasonality=none) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["row_count"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days,
      days_back=days_back,
      where_expression=where_expression,
      time_bucket=time_bucket,
      seasonality=seasonality,
      anomaly_direction=anomaly_direction
    )
  }}
{% endtest %}
