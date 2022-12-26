{% test freshness_anomalies(model, data_timestamp_column, insertion_timestamp_column, sensitivity, backfill_days, where_expression, time_bucket) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["freshness_v2"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days,
      where_expression=where_expression,
      time_bucket=time_bucket
    )
  }}
{% endtest %}
