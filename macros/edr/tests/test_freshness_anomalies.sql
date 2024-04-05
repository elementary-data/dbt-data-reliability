{% test freshness_anomalies(model, timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity, detection_delay, anomaly_exclude_metrics, detection_period, training_period, dbt_model_id) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["freshness"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      where_expression=where_expression,
      anomaly_sensitivity=anomaly_sensitivity,
      min_training_set_size=min_training_set_size,
      time_bucket=time_bucket,
      days_back=days_back,
      backfill_days=backfill_days,
      mandatory_params=['timestamp_column'],
      seasonality=seasonality,
      sensitivity=sensitivity,
      detection_delay=detection_delay,
      anomaly_exclude_metrics=anomaly_exclude_metrics,
      detection_period=detection_period,
      training_period=training_period,
      dbt_model_id=dbt_model_id
    )
  }}
{% endtest %}
