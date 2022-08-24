{%- macro get_anomaly_query(anomaly_scores_test_table_relation, sensitivity, backfill_days) -%}
    {%- set backfill_period = "'-" ~ backfill_days ~ "'" %}
    {%- set anomaly_query -%}
        select
            *,
            {{ elementary.anomaly_detection_description() }}
        from {{ anomaly_scores_test_table_relation }}
        where abs(anomaly_score) > {{ sensitivity }}
            {# get anomalies only for a limited period called the backfill period #}
            and bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }}
            {# to avoid false positives return only anomaly scores that were calculated with a big enough training set #}
            and training_set_size >= {{ elementary.get_config_var('days_back') -1 }}
    {%- endset -%}
    {{- return(anomaly_query) -}}
{%- endmacro -%}