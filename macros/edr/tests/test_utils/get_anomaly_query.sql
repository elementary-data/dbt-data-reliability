{%- macro get_anomaly_query(flattened_test=none) -%}
  {%- set query -%}
    select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }}) results
    where is_anomalous = true
  {%- endset -%}
  {{- return(query) -}}
{%- endmacro -%}

{% macro get_read_anomaly_scores_query(flattened_test=none) %}
    {% if not flattened_test %}
      {% set flattened_test = elementary.flatten_test(model) %}
    {% endif %}

    {% set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=flattened_test.test_params.sensitivity) %}
    {% set anomaly_direction = elementary.get_test_argument(argument_name='anomaly_direction', value=flattened_test.test_params.anomaly_direction) | lower %}
    {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=flattened_test.test_params.backfill_days) %}
    {%- set backfill_period = "'-" ~ backfill_days ~ "'" %}

    {%- set anomaly_query -%}
      with anomaly_scores as (
        select
            id,
            metric_id,
            test_execution_id,
            test_unique_id,
            detected_at,
            full_table_name,
            column_name,
            metric_name,
            anomaly_score,
            anomaly_score_threshold,
            anomalous_value,
            bucket_start,
            bucket_end,
            bucket_seasonality,
            metric_value,
            min_metric_value,
            max_metric_value,
            training_avg,
            training_stddev,
            training_set_size,
            training_start,
            training_end,
            dimension,
            dimension_value,
            {{ elementary.anomaly_detection_description() }},
            max(bucket_end) as max_bucket_end
        from {{ elementary.get_elementary_test_table(elementary.get_elementary_test_table_name(), 'anomaly_scores') }}
        {{ dbt_utils.group_by(25) }}
      ),
      anomaly_scores_with_is_anomalous as (
        select
          *,
          case when
            anomaly_score is not null and
            {{ elementary.is_score_anomalous_condition(sensitivity, anomaly_direction) }} and
            bucket_end >= {{ elementary.edr_timeadd('day', backfill_period, 'max_bucket_end') }} and
            training_set_size >= {{ elementary.get_config_var('min_training_set_size') }}
          then TRUE else FALSE end as is_anomalous
        from anomaly_scores
      )

      select
        metric_value as value,
        training_avg as average,
        {# when there is an anomaly we would want to use the last value of the metric (lag), otherwise visually the expectations would look out of bounds #}
        case
        when is_anomalous = TRUE and '{{ anomaly_direction }}' = 'spike' then
         lag(metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        when is_anomalous = TRUE and '{{ anomaly_direction }}' != 'spike' then
         lag(min_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        when '{{ anomaly_direction }}' = 'spike' then metric_value
        else min_metric_value end as min_value,
        case
        when is_anomalous = TRUE and '{{ anomaly_direction }}' = 'drop' then
         lag(metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        when is_anomalous = TRUE and '{{ anomaly_direction }}' != 'drop' then
         lag(max_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
        when '{{ anomaly_direction }}' = 'drop' then metric_value
        else max_metric_value end as max_value,
        bucket_start as start_time,
        bucket_end as end_time,
        *
      from anomaly_scores_with_is_anomalous
      order by bucket_end, dimension_value
    {%- endset -%}
    {{- return(anomaly_query) -}}
{% endmacro %}

{%- macro set_directional_anomaly(anomaly_direction, anomaly_score, sensitivity) -%}
    {% if anomaly_direction | lower == 'spike' %}
        anomaly_score > {{ sensitivity }}
    {% elif anomaly_direction | lower == 'drop' %}
        anomaly_score < {{ sensitivity * -1 }}
    {% else %}
        abs(anomaly_score) > {{ sensitivity }}
    {% endif %}
{% endmacro %}

{%- macro is_score_anomalous_condition(sensitivity, anomaly_direction) -%}
    {%- set spikes_only_metrics = ['freshness', 'event_freshness'] -%}
    case when metric_name IN {{ elementary.strings_list_to_tuple(spikes_only_metrics) }} then
            anomaly_score > {{ sensitivity }}
    else
        {{ elementary.set_directional_anomaly(anomaly_direction, anomaly_score, sensitivity) }}
     end
{%- endmacro -%}
