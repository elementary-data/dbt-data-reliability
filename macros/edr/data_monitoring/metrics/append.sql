{% macro append_metrics(bucketed_metrics_query, timestamp_column, days_back, where) %}
  {% set flattened_test = elementary.flatten_test(model) %}
  {% set metrics_start_date = (elementary.get_run_started_at() - modules.datetime.timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0) %}
  {% set query %}
    with bucketed_metrics as (
      {{ bucketed_metrics_query }}
    ),
    filtered_metrics as (
      select *
      from bucketed_metrics
      where metric_bucket_start >= {{ elementary.cast_as_timestamp(elementary.quote(metrics_start_date)) }}
      {% if where %} and {{ where }} {% endif %}
    ),
    daily_buckets as (
      {{ elementary.daily_buckets_cte() }}
    ),
    daily_metrics as (
      select
        edr_daily_bucket,
        coalesce({{ elementary.cast_as_float("metric_value") }}, 0) as metric_value
      from daily_buckets left join filtered_metrics on edr_daily_bucket = metric_bucket_start
    )

    select
      *,
      {{ elementary.quote(flattened_test.short_name) }} as metric_name,
      {{ elementary.current_timestamp_in_utc() }} as updated_at
    from daily_metrics
  {% endset %}
  {% do return(query) %}
{% endmacro %}
