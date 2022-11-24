{% test freshness(model, timestamp_column=none, sensitivity=none, backfill_days=none) %}
  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["freshness"],
      freshness_column=none,
      timestamp_column=timestamp_column,
      sensitivity=sensitivity,
      backfill_days=backfill_days
    )
  }}
{% endtest %}
