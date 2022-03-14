{% macro get_anomaly_alerts_query(full_table_name, monitors, column_name = none) %}

-- TODO: now we get anomalies from last 7 days. should we change this somehow? maybe since min_bucket_start?

    {% set anomaly_alerts_query %}
        with anomalies as (

            select *,
                {{ elementary.anomaly_detection_description() }}
            from {{ ref('metrics_anomaly_score') }}
                where is_anomaly = true and upper(full_table_name) = upper('{{ full_table_name }}')
                 and metric_name in {{ elementary.strings_list_to_tuple(monitors) }}
                {% if column_name %}
                    and upper(column_name) = upper('{{ column_name }}')
                {% endif %}

        ),

        anomaly_alerts as (

            select
                id as alert_id,
                updated_at as detected_at,
                {{ elementary.full_name_split('database_name') }},
                {{ elementary.full_name_split('schema_name') }},
                {{ elementary.full_name_split('table_name') }},
                column_name,
                'anomaly_detection' as alert_type,
                metric_name as sub_type,
                description as alert_description,
                {{ elementary.null_string() }} as owner,
                {{ elementary.null_string() }} as tags,
                {{ elementary.null_string() }} as alert_results_query,
                {{ elementary.null_string() }} as other
            from anomalies

        )

        select * from anomaly_alerts
    {% endset %}
    {{ return(anomaly_alerts_query) }}
{% endmacro %}