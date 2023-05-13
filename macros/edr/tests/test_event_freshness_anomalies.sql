-- TODO: Anomaly direction should be here?
{% test event_freshness_anomalies(model, event_timestamp_column, update_timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days) %}
  {% if execute %}
    {%- if not event_timestamp_column -%}
      {%- do exceptions.raise_compiler_error('event_timestamp_column must be specified for the event freshness test!') -%}
    {%- endif -%}
-- TODO: This check happens twice if the exception is here and not in the configuration macro
    {%- set event_timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model, event_timestamp_column) -%}
    {%- if not elementary.get_is_column_timestamp(model_relation, event_timestamp_column, event_timestamp_column_data_type) -%}
      {% do exceptions.raise_compiler_error("Column `{}` is not a timestamp.".format(event_timestamp_column)) %}
    {%- endif -%}
  {% endif %}

  {{ elementary.test_table_anomalies(
      model=model,
      table_anomalies=["event_freshness"],
      timestamp_column=update_timestamp_column,
      where_expression=where_expression,
      anomaly_sensitivity=anomaly_sensitivity,
      min_training_set_size=min_training_set_size,
      time_bucket=time_bucket
      days_back=days_back,
      backfill_days=backfill_days,
      event_timestamp_column=event_timestamp_column
    )
  }}
{% endtest %}
