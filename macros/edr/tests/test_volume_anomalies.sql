{% test volume_anomalies(model, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity, ignore_small_changes, fail_on_zero, detection_delay, anomaly_exclude_metrics, detection_period, training_period) %}
  {{ config(tags = ['elementary-tests']) }}

  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["row_count"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      where_expression=where_expression,
      anomaly_sensitivity=anomaly_sensitivity,
      anomaly_direction=anomaly_direction,
      min_training_set_size=min_training_set_size,
      time_bucket=time_bucket,
      days_back=days_back,
      backfill_days=backfill_days,
      seasonality=seasonality,
      sensitivity=sensitivity,
      ignore_small_changes=ignore_small_changes,
      fail_on_zero=fail_on_zero,
      detection_delay=detection_delay,
      anomaly_exclude_metrics=anomaly_exclude_metrics,
      detection_period=detection_period,
      training_period=training_period
    )
  }}
{% endtest %}
