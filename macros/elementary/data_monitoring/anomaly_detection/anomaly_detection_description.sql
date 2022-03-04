{% macro anomaly_detection_description() %}
    case
        when metric_name = 'freshness' then {{ elementary.freshness_description() }}
        when column_name is null then {{ elementary.table_metric_description() }}
        when column_name is not null then {{ elementary.column_metric_description() }}
        else null
    end as description
{% endmacro %}

{% macro freshness_description() %}
    'The table ' || full_table_name || ' last update was ' || round(latest_value,2) || ' hours ago. The average for this metric is ' || round(metric_avg,2) || '.'
{% endmacro %}

{% macro table_metric_description() %}
    'The table ' || full_table_name || ' last ' || metric_name || ' value is ' || round(latest_value,3) ||
    '. The anomaly score is ' || round(z_score, 3) || ' and the average for this metric is ' || round(metric_avg,3) || '.'
{% endmacro %}


{% macro column_metric_description() %}
    'The column ' || column_name || ' in table ' || full_table_name || ' last ' || metric_name || ' value is ' || round(latest_value,3) ||
    '. The anomaly score is ' || round(z_score, 3) || ' and the average for this metric is ' || round(metric_avg,3) || '.'
{% endmacro %}