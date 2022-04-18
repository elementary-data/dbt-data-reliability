{% macro empty_alerts() %}
    {{ elementary.empty_table([('alert_id','string'),('detected_at','timestamp'),('database_name','string'),('schema_name','string'),('table_name','string'),('column_name','string'),('alert_type','string'),('sub_type','string'),('alert_description','string'),('owner','string'),('tags','string'),('alert_results_query','string'),('other','string')]) }}
{% endmacro %}

{% macro empty_data_monitors() %}
    {{ elementary.empty_table([('full_table_name','string'),('column_name','string'),('metric_name','string'),('metric_value','float'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int')]) }}
{% endmacro %}

{% macro empty_column_unpivot_cte() %}
    {{ elementary.empty_table([('edr_column_name','string'),('edr_bucket','timestamp'),('metric_name','string'),('metric_value','float')]) }}
{% endmacro %}

{% macro empty_data_monitoring_metrics() %}
    {{ elementary.empty_table([('id','string'),('full_table_name','string'),('column_name','string'),('metric_name','string'),('metric_value','float'),('source_value','string'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int'),('updated_at','timestamp')]) }}
{% endmacro %}

{% macro empty_test_anomalies() %}
    {{ elementary.empty_table([('id','string'),('full_table_name','string'),('column_name','string'),('metric_name','string'),('z_score','float'),('latest_metric_value','float'),('bucket_start','timestamp'),('bucket_end','timestamp'),('training_avg','float'),('training_stddev','float'),('training_set_size','int')]) }}
{% endmacro %}

{% macro empty_column_monitors_cte() %}
    {%- set column_monitors_list = elementary.all_column_monitors() %}
    {%- set columns_definition = [('column_name', 'string'), ('bucket', 'timestamp')] %}
    {%- for monitor in column_monitors_list %}
        {%- do columns_definition.append((monitor,'int'))-%}
    {%- endfor %}
    {{ elementary.empty_table(columns_definition) }}
{% endmacro %}


{% macro empty_table(column_name_and_type_list) %}

    {%- set empty_table_query -%}
        with empty_table as (
            select
            {% for column in column_name_and_type_list %}
                {{ elementary.empty_column(column[0], column[1]) }} {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
            )
        select * from empty_table
        where 1 = 0
    {%- endset -%}

    {{- return(empty_table_query)-}}

{% endmacro %}


{% macro empty_column(column_name, data_type) %}

    {%- set dummy_values = elementary.dummy_values() %}

    {%- if data_type == 'boolean' %}
        cast ({{ dummy_values['boolean'] }} as {{ elementary.type_bool()}}) as {{ column_name }}
    {%- elif data_type == 'timestamp' -%}
        cast('{{ dummy_values['timestamp'] }}' as {{ dbt_utils.type_timestamp() }}) as {{ column_name }}
    {%- elif data_type == 'int' %}
        cast({{ dummy_values['int'] }} as {{ dbt_utils.type_int() }}) as {{ column_name }}
    {%- elif data_type == 'float' %}
        cast({{ dummy_values['float'] }} as {{ dbt_utils.type_float() }}) as {{ column_name }}
    {%- elif data_type == 'long_string' %}
        cast('{{ dummy_values['long_string'] }}' as {{ elementary.type_long_string() }}) as {{ column_name }}
    {%- else %}
        cast('{{ dummy_values['string'] }}' as {{ elementary.type_string() }}) as {{ column_name }}
    {%- endif %}

{% endmacro %}


{% macro dummy_values() %}

    {%- set dummy_values = {
     'string': "dummy_string",
     'long_string': "this_is_just_a_long_dummy_string",
     'boolean': 'null',
     'int': 123456789,
     'float': 123456789.99,
     'timestamp': "2091-02-17"
    } %}

    {{ return(dummy_values) }}

{% endmacro %}