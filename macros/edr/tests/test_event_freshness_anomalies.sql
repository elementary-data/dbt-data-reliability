{% test event_freshness_anomalies(model, event_timestamp_column, update_timestamp_column, where_expression, anomaly_sensitivity, anomaly_direction, min_training_set_size, time_bucket, days_back, backfill_days, seasonality, sensitivity, ignore_small_changes, detection_delay, anomaly_exclude_metrics, detection_period, training_period) %}
  {{ config(tags = ['elementary-tests']) }}
  {% if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
    {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
    {% if not model_relation %}
        {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
    {% endif %}

    {%- if not event_timestamp_column -%}
      {%- do exceptions.raise_compiler_error('event_timestamp_column must be specified for the event freshness test!') -%}
    {%- endif -%}
    {%- set event_timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model_relation, event_timestamp_column) -%}
    {%- if not elementary.is_column_timestamp(model_relation, event_timestamp_column, event_timestamp_column_data_type) -%}
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
      time_bucket=time_bucket,
      days_back=days_back,
      backfill_days=backfill_days,
      event_timestamp_column=event_timestamp_column,
      mandatory_params=['event_timestamp_column'],
      seasonality=seasonality,
      sensitivity=sensitivity,
      ignore_small_changes=ignore_small_changes,
      detection_delay=detection_delay,
      anomaly_exclude_metrics=anomaly_exclude_metrics,
      detection_period=detection_period,
      training_period=training_period
    )
  }}
{% endtest %}
