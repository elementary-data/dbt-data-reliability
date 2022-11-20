{% test volume(
  model,
  timestamp_column=none,
  days_back=elementary.get_config_var("days_back"),
  where=none,
  sensitivity=none
) %}
  {% if not execute or flags.WHICH not in ["test", "build"] %}
    {% do return(none) %}
  {% endif %}

  {% set bucketed_metrics_query %}
    select
      count(*) as metric_value,
      {{ elementary.time_trunc("day", timestamp_column) }} as metric_bucket_start
    from {{ model }}
    group by metric_bucket_start
  {% endset %}

  {{ elementary.test_anomalies(bucketed_metrics_query, timestamp_column, days_back, where, sensitivity) }}
{% endtest %}
