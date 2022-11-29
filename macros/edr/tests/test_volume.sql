{% test volume(model, timestamp_column=none, sensitivity=none, backfill_days=none, where=none) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["row_count"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days,
      where=where
    )
  }}
{% endtest %}
