{% macro anomaly_detection_description() %}
    case
        when dimension is not null then {{ elementary.dimension_metric_description() }}
        when metric_name = 'freshness' then {{ elementary.freshness_description() }}
        when column_name is null then {{ elementary.table_metric_description() }}
        when column_name is not null then {{ elementary.column_metric_description() }}
        else null
    end as anomaly_description
{% endmacro %}

{% macro freshness_description() %}
    'Last update was at ' || anomalous_value || ', ' || abs(round(metric_value/3600,2)) || ' hours ago. Usually the table is updated within ' || abs(round(training_avg/3600,2)) || ' hours.'
{% endmacro %}

{% macro table_metric_description() %}
    'The last ' || metric_name || ' value is ' || round(metric_value,3) ||
    '. The average for this metric is ' || round(training_avg,3) || '.'
{% endmacro %}

{% macro column_metric_description() %}
    'In column ' || column_name || ', the last ' || metric_name || ' value is ' || round(metric_value,3) ||
    '. The average for this metric is ' || round(training_avg,3) || '.'
{% endmacro %}

{% macro dimension_metric_description() %}
    'The last ' || metric_name || ' value for dimension ' || dimension || ' - ' ||
    case when dimension_value is null then 'NULL' else dimension_value end || ' is ' || round(metric_value,3) ||
    '. The average for this metric is ' || round(training_avg,3) || '.'
{% endmacro %}
