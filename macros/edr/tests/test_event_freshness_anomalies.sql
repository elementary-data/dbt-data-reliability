{% test event_freshness_anomalies(model, event_timestamp_column, update_timestamp_column, sensitivity, backfill_days, where_expression, time_bucket) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["event_freshness"],
      event_timestamp_column=event_timestamp_column,
      timestamp_column=update_timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days,
      where_expression=where_expression,
      time_bucket=time_bucket
    )
  }}
{% endtest %}
