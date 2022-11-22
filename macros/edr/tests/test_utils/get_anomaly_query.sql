{%- macro get_anomaly_query(flattened_test=none) -%}
  {%- set query -%}
    select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }})
    where is_anomalous = true
  {%- endset -%}
  {{- return(query) -}}
{%- endmacro -%}

{% macro get_read_anomaly_scores_query(flattened_test=none) %}
    {% if not flattened_test %}
      {% set flattened_test = elementary.flatten_test(model) %}
    {% endif %}

    {% set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=flattened_test.test_params.sensitivity) %}
    {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=flattened_test.test_params.backfill_days) %}
    {%- set backfill_period = "'-" ~ backfill_days ~ "'" %}
    {%- set anomaly_query -%}
      with anomaly_scores as (
        select
          *,
          {{ elementary.anomaly_detection_description() }}
        from {{ elementary.get_elementary_test_table(flattened_test.name, 'anomaly_scores') }}
      ),
      anomaly_scores_with_is_anomalous as (
        select
          *,
          case when
            anomaly_score is not null and
            abs(anomaly_score) > {{ sensitivity }} and
            bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }} and
            training_set_size >= {{ elementary.get_config_var('days_back') -1 }}
          then TRUE else FALSE end as is_anomalous
        from anomaly_scores
      )

      select
        metric_value as value,
        training_avg as average,
        case when is_anomalous = TRUE then
         lag(min_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        else min_metric_value end as min_value,
        case when is_anomalous = TRUE then
         lag(max_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        else max_metric_value end as max_value,
        bucket_start as start_time,
        bucket_end as end_time,
        *
      from anomaly_scores_with_is_anomalous
      order by bucket_end, dimension_value
    {%- endset -%}
    {{- return(anomaly_query) -}}
{% endmacro %}
