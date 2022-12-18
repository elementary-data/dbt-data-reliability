{% test volume_anomalies(model, timestamp_column=none, sensitivity=none, backfill_days=none, where_expression=none) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["row_count"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days,
      where_expression=where_expression
    )
  }}
{% endtest %}
