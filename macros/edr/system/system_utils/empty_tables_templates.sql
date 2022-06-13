{% macro empty_alerts() %}
    {{ elementary.empty_table([('alert_id','long_string'),('data_issue_id','string'),('test_execution_id','long_string'),('test_unique_id','long_string'),('detected_at','timestamp'),('database_name','string'),('schema_name','string'),('table_name','string'),('column_name','string'),('alert_type','string'),('sub_type','string'),('alert_description','long_string'),('owners','string'),('tags','string'),('alert_results_query','long_string'),('other','string'),('test_name','long_string'),('test_params','long_string'),('severity','string'),('status','string')]) }}
{% endmacro %}

{% macro empty_column_unpivot_cte() %}
    {{ elementary.empty_table([('edr_column_name','string'),('edr_bucket','timestamp'),('metric_name','string'),('metric_value','float')]) }}
{% endmacro %}

{% macro empty_data_monitoring_metrics() %}
    {{ elementary.empty_table([('id','string'),('full_table_name','string'),('column_name','string'),('metric_name','string'),('metric_value','float'),('source_value','string'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int'),('updated_at','timestamp')]) }}
{% endmacro %}


{% macro empty_column_monitors_cte() %}
    {%- set column_monitors_list = elementary.all_column_monitors() %}
    {%- set columns_definition = [('column_name', 'string'), ('bucket', 'timestamp')] %}
    {%- for monitor in column_monitors_list %}
        {%- do columns_definition.append((monitor,'int'))-%}
    {%- endfor %}
    {{ elementary.empty_table(columns_definition) }}
{% endmacro %}