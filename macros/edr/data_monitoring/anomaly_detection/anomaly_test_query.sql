{% macro anomaly_test_query(metric_name) %}
    {%- set anomaly_test_query %}
        with metrics_for_anomalies as (

            select * from {{ ref('metrics_for_anomalies') }}

        ),

        anomaly_detection as (

         select
             *,
             {{ elementary.anomaly_detection_description() }}
         from metrics_for_anomalies
         where abs(z_score) > {{ var('anomaly_score_threshold') }}
            and metric_name = '{{ metric_name }}'

        )

        select * from anomaly_detection
    {%- endset %}

    {{ return(anomaly_test_query) }}

{% endmacro %}