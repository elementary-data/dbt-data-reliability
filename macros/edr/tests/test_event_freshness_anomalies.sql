{% test event_freshness_anomalies(model, event_timestamp_column, update_timestamp_column, sensitivity, backfill_days, where_expression, time_bucket) %}
  {% if execute %}
    {%- if not event_timestamp_column -%}
      {%- do exceptions.raise_compiler_error('event_timestamp_column must be specified for the event freshness test!') -%}
    {%- endif -%}

    {%- set event_timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model, event_timestamp_column) -%}
    {%- if not elementary.get_is_column_timestamp(model_relation, event_timestamp_column, event_timestamp_column_data_type) -%}
      {% do exceptions.raise_compiler_error("Column `{}` is not a timestamp.".format(event_timestamp_column)) %}
    {%- endif -%}
  {% endif %}

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
