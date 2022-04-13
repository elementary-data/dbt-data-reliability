{% macro empty_alerts() %}
    {{ elementary.empty_table([('alert_id','str'),('detected_at','timestamp'),('database_name','str'),('schema_name','str'),('table_name','str'),('column_name','str'),('alert_type','str'),('sub_type','str'),('alert_description','str'),('owner','str'),('tags','str'),('alert_results_query','str'),('other','str')]) }}
{% endmacro %}

{% macro empty_data_monitors() %}
    {{ elementary.empty_table([('full_table_name','str'),('column_name','str'),('metric_name','str'),('metric_value','float'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int')]) }}
{% endmacro %}

{% macro empty_column_unpivot_cte() %}
    {{ elementary.empty_table([('edr_column_name','str'),('edr_bucket','timestamp'),('metric_name','str'),('metric_value','float')]) }}
{% endmacro %}

{% macro empty_data_monitoring_metrics() %}
    {{ elementary.empty_table([('id','str'),('full_table_name','str'),('column_name','str'),('metric_name','str'),('metric_value','float'),('source_value','str'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int'),('updated_at','timestamp')]) }}
{% endmacro %}

{% macro empty_test_anomalies() %}
    {{ elementary.empty_table([('id','string'),('full_table_name','str'),('column_name','str'),('metric_name','str'),('z_score','float'),('latest_metric_value','float'),('bucket_start','timestamp'),('bucket_end','timestamp'),('training_avg','float'),('training_stddev','float'),('training_set_size','int')]) }}
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
    {%- set first_column_name = column_name_and_type_list[0][0] %}
    {%- set first_column_type = column_name_and_type_list[0][1] %}
    {%- set first_column_value = elementary.dummy_values()[first_column_type]  %}

    {%- set empty_table_query -%}
        with empty_table as (
            select
            {% for column in column_name_and_type_list %}
                {{ elementary.empty_column(column[0], column[1]) }} {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
            )
        select * from empty_table
        where {{ first_column_name }} != {{ first_column_value }}
    {%- endset -%}

    {{- return(empty_table_query)-}}

{% endmacro %}


{% macro empty_column(column_name, data_type) %}

    {%- set dummy_values = elementary.dummy_values() %}

    {%- if data_type == 'boolean' %}
        cast ({{ dummy_values['bool'] }} as {{ elementary.type_bool()}}) as {{ column_name }}
    {%- elif data_type == 'timestamp' -%}
        cast({{ dummy_values['timestamp'] }} as {{ dbt_utils.type_timestamp() }}) as {{ column_name }}
    {%- elif data_type == 'int' %}
        cast({{ dummy_values['int'] }} as {{ dbt_utils.type_int() }}) as {{ column_name }}
    {%- elif data_type == 'float' %}
        cast({{ dummy_values['float'] }} as {{ dbt_utils.type_float() }}) as {{ column_name }}
    {%- else %}
        cast({{ dummy_values['str'] }} as {{ elementary.type_string() }}) as {{ column_name }}
    {%- endif %}

{% endmacro %}


{% macro dummy_values() %}

    {%- set dummy_values = {
     'str': "'this_is_just_a_very_very_long_dummy_string_so_we_could_create_empty_tables_using_dbt_and_insert_data_later'",
     'bool': 'null',
     'int': 123456789,
     'float': 123456789.99,
     'timestamp': "'2091-02-17'"
    } %}

    {{ return(dummy_values) }}

{% endmacro %}