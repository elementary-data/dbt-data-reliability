{%- macro get_anomaly_query(flattened_test) -%}
    {% set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=flattened_test.test_params.sensitivity) %}
    {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=flattened_test.test_params.backfill_days) %}
    {% set backfill_period = "'" ~ backfill_days ~ "'" %}
    {%- set anomaly_query -%}
        select
            *,
            {{ elementary.anomaly_detection_description() }}
        from {{ elementary.get_elementary_test_table(flattened_test.name, 'anomaly_scores') }}
        where abs(anomaly_score) > {{ sensitivity }}
            {# get anomalies only for a limited period called the backfill period #}
            and bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }}
            {# to avoid false positives return only anomaly scores that were calculated with a big enough training set #}
            and training_set_size >= {{ elementary.get_config_var('days_back') -1 }}
    {%- endset -%}
    {{- return(anomaly_query) -}}
{%- endmacro -%}