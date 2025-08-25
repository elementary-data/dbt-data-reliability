{%- macro get_anomaly_query(flattened_test=none) -%}
  {%- set query -%}
    select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }}) results
    where is_anomalous = true
  {%- endset -%}
  {{- return(query) -}}
{%- endmacro -%}

{%- macro get_anomaly_query_for_dimension_anomalies(flattened_test=none) -%}
  {%- set dimension_values_query -%}
    select distinct dimension_value from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }}) results
    where is_anomalous = true
  {%- endset -%}

  {% set dimension_anomalies_query -%}
    select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }}) results
    where dimension_value in ({{ dimension_values_query }})
  {%- endset -%}

  {{- return(dimension_anomalies_query) -}}
{%- endmacro -%}

{% macro get_read_anomaly_scores_query(flattened_test=none) %}
    {% if not flattened_test %}
      {% set flattened_test = elementary.flatten_test(model) %}
    {% endif %}
    {%- set test_unique_id = flattened_test.unique_id %}
    {%- set test_configuration = elementary.get_cache(test_unique_id) %}
    {%- if not test_configuration %}
        {{ exceptions.raise_compiler_error("Failed to load configuration dict from cache for test `{}`".format(test_unique_id)) }}
    {%- endif %}
    {%- set backfill_period = "'-" ~ test_configuration.backfill_days ~ "'" %}

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
            max(bucket_end) over (partition by test_execution_id) as max_bucket_end
        from {{ elementary.get_elementary_test_table(elementary.get_elementary_test_table_name(), 'anomaly_scores') }}
      ),
      anomaly_scores_with_is_anomalous as (
        select
          *,
case when
          (
            {{ elementary.anomaly_score_condition(test_configuration) }}
          )
          and bucket_end > {{ elementary.edr_timeadd('day', backfill_period, 'max_bucket_end') }}
          then TRUE else FALSE end as is_anomalous
        from anomaly_scores
      ),

      final_results as (
        select
            metric_value as {{ elementary.escape_reserved_keywords('value') }},
            training_avg as average,
            {# when there is an anomaly we would want to use the last value of the metric (lag), otherwise visually the expectations would look out of bounds #}
            case
                when is_anomalous = TRUE and '{{ test_configuration.anomaly_direction }}' = 'spike' then
                    {{ elementary.lag('metric_value') }} over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
                when is_anomalous = TRUE and '{{ test_configuration.anomaly_direction }}' != 'spike' then
                    {{ elementary.lag('min_metric_value') }} over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
                when '{{ test_configuration.anomaly_direction }}' = 'spike' then metric_value
                else min_metric_value
            end as min_value,
            case
                when is_anomalous = TRUE and '{{ test_configuration.anomaly_direction }}' = 'drop' then
                    {{ elementary.lag('metric_value') }} over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
                when is_anomalous = TRUE and '{{ test_configuration.anomaly_direction }}' != 'drop' then
                    {{ elementary.lag('max_metric_value') }} over (partition by full_table_name, column_name, metric_name, dimension, dimension_value, bucket_seasonality order by bucket_end)
                when '{{ test_configuration.anomaly_direction }}' = 'drop' then metric_value
                else max_metric_value
            end as max_value,
            bucket_start as start_time,
            bucket_end as end_time,
            *
        from anomaly_scores_with_is_anomalous
        order by bucket_end, dimension_value
    )

      select * from final_results
      where {{ test_configuration.exclude_final_results }}
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

{%- macro avg_percent_anomalous_condition(spike_failure_percent_threshold, drop_failure_percent_threshold, anomaly_direction) -%}
  (
  {% set spike_filter %}
    (metric_value > ((1 + {{ spike_failure_percent_threshold }}/100.0) * training_avg))
  {% endset %}
  {% set drop_filter %}
    (metric_value < ((1 - {{ drop_failure_percent_threshold }}/100.0) * training_avg))
  {% endset %}

  {% if spike_failure_percent_threshold and drop_failure_percent_threshold and (anomaly_direction | lower) == 'both' %}
      {{ spike_filter }} or {{ drop_filter }}
  {% else %}
    {% if spike_failure_percent_threshold and anomaly_direction | lower in ['spike', 'both'] %}
      {{ spike_filter }}
    {% else %}
        (1 = 1)
    {% endif %}

    and

    {% if drop_failure_percent_threshold and anomaly_direction | lower in ['drop', 'both'] %}
        {{ drop_filter }}
    {% else %}
        (1 = 1)
    {% endif %}
  {% endif %}
  )
{%- endmacro -%}

{% macro fail_on_zero(fail_on_zero) %}
  (
    metric_value = 0 and
    {% if fail_on_zero %}
      1 = 1
    {% else %}
      1 = 2
    {% endif %}
  )
{% endmacro %}

{% macro anomaly_score_condition(test_configuration) %}
  (anomaly_score is not null and
  (
    {{ elementary.fail_on_zero(test_configuration.fail_on_zero) }} or
    (
      {{ elementary.is_score_anomalous_condition(test_configuration.anomaly_sensitivity, test_configuration.anomaly_direction) }} and
      {{ elementary.avg_percent_anomalous_condition(
          test_configuration.ignore_small_changes.spike_failure_percent_threshold,
          test_configuration.ignore_small_changes.drop_failure_percent_threshold,
          test_configuration.anomaly_direction
        )
      }}
    )
  ))
{% endmacro %}
