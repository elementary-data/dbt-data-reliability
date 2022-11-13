{%- macro get_anomaly_query(flattened_test=none) -%}
  {% if not flattened_test %}
    {% set flattened_test = elementary.flatten_test(model) %}
  {% endif %}

  {% set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=flattened_test.test_params.sensitivity) %}
  {%- set query -%}
    select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }})
    where abs(anomaly_score) > {{ sensitivity }}
  {%- endset -%}
  {{- return(query) -}}
{%- endmacro -%}

{% macro get_read_anomaly_scores_query(flattened_test=none) %}
    {% if not flattened_test %}
      {% set flattened_test = elementary.flatten_test(model) %}
    {% endif %}

    {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=flattened_test.test_params.backfill_days) %}
    {%- set backfill_period = "'-" ~ backfill_days ~ "'" %}
    {%- set anomaly_query -%}
        select
            *,
            {{ elementary.anomaly_detection_description() }}
        from {{ elementary.get_elementary_test_table(flattened_test.name, 'anomaly_scores') }}
        where
            {# get anomalies only for a limited period called the backfill period #}
            bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }} and
            {# to avoid false positives return only anomaly scores that were calculated with a big enough training set #}
            training_set_size >= {{ elementary.get_config_var('days_back') -1 }}
    {%- endset -%}
    {{- return(anomaly_query) -}}
{% endmacro %}
