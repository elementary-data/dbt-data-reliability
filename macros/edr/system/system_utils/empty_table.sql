{% macro empty_elementary_test_results() %}
    {{ elementary.empty_table([
    ('id','long_string'),
    ('data_issue_id','string'),
    ('test_execution_id','long_string'),
    ('test_unique_id','long_string'),
    ('model_unique_id','long_string'),
    ('invocation_id', 'string'),
    ('detected_at','timestamp'),
    ('created_at','timestamp'),
    ('database_name','string'),
    ('schema_name','string'),
    ('table_name','string'),
    ('column_name','string'),
    ('test_type','string'),
    ('test_sub_type','string'),
    ('test_results_description','long_string'),
    ('owners','string'),
    ('tags','string'),
    ('test_results_query','long_string'),
    ('other','string'),
    ('test_name','long_string'),
    ('test_params','long_string'),
    ('severity','string'),
    ('status','string'),
    ('failures', 'bigint'),
    ('test_short_name', 'string'),
    ('test_alias', 'string'),
    ('result_rows', 'long_string'),
    ('failed_row_count', 'bigint')
    ]) }}
{% endmacro %}

{% macro empty_dbt_source_freshness_results() %}
    {{ elementary.empty_table([
    ('source_freshness_execution_id','string'),
    ('unique_id','string'),
    ('max_loaded_at','string'),
    ('snapshotted_at','string'),
    ('generated_at', 'string'),
    ('created_at', 'timestamp'),
    ('max_loaded_at_time_ago_in_s','float'),
    ('status','string'),
    ('error','string'),
    ('compile_started_at', 'string'),
    ('compile_completed_at', 'string'),
    ('execute_started_at', 'string'),
    ('execute_completed_at', 'string'),
    ('invocation_id', 'string'),
    ('warn_after', 'string'),
    ('error_after', 'string'),
    ('filter', 'long_string')
    ]) }}
{% endmacro %}

{% macro empty_data_monitoring_metrics(with_created_at=true) %}
    {{ return(adapter.dispatch('empty_data_monitoring_metrics', 'elementary')(with_created_at)) }}
{% endmacro %}

{% macro default__empty_data_monitoring_metrics(with_created_at=true) %}
    {% set columns = [('id','string'),
                      ('full_table_name','string'),
                      ('column_name','string'),
                      ('metric_name','string'),
                      ('metric_type','string'),
                      ('metric_value','float'),
                      ('source_value','string'),
                      ('bucket_start','timestamp'),
                      ('bucket_end','timestamp'),
                      ('bucket_duration_hours','int'),
                      ('updated_at','timestamp'),
                      ('dimension','string'),
                      ('dimension_value','string'),
                      ('metric_properties','string')]
    %}
    {% if with_created_at %}
        {% do columns.append(('created_at','timestamp')) %}
    {% endif %}
    {{ elementary.empty_table(columns) }}
{% endmacro %}

{% macro clickhouse__empty_data_monitoring_metrics(with_created_at=true) %}
    {% set columns = [('id','string'),
                      ('full_table_name','nullable(string)'),
                      ('column_name','nullable(string)'),
                      ('metric_name','nullable(string)'),
                      ('metric_type','nullable(string)'),
                      ('metric_value','nullable(float)'),
                      ('source_value','nullable(string)'),
                      ('bucket_start','timestamp'),
                      ('bucket_end','timestamp'),
                      ('bucket_duration_hours','nullable(int)'),
                      ('updated_at','nullable(timestamp)'),
                      ('dimension','nullable(string)'),
                      ('dimension_value','nullable(string)'),
                      ('metric_properties','string')]
    %}
    {% if with_created_at %}
        {% do columns.append(('created_at','nullable(timestamp)')) %}
    {% endif %}
    {{ elementary.empty_table(columns) }}
{% endmacro %}

{% macro empty_schema_columns_snapshot() %}
    {{ elementary.empty_table([('column_state_id','string'),('full_column_name','string'),('full_table_name','string'),('column_name','string'),('data_type','string'),('is_new','boolean'),('detected_at','timestamp'),('created_at','timestamp')]) }}
{% endmacro %}


{% macro empty_column_monitors_cte() %}
    {%- set column_monitors_list = elementary.get_agg_column_monitors() %}
    {%- set columns_definition = [('column_name', 'string'), ('bucket', 'timestamp')] %}
    {%- for monitor in column_monitors_list %}
        {%- do columns_definition.append((monitor,'int'))-%}
    {%- endfor %}
    {{ elementary.empty_table(columns_definition) }}
{% endmacro %}


{% macro empty_table(column_name_and_type_list) %}

    {%- set empty_table_query -%}
        select * from (
            select
            {% for column in column_name_and_type_list %}
                {{ elementary.empty_column(column[0], column[1]) }} {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
        ) as empty_table
        where 1 = 0
    {%- endset -%}

    {{- return(empty_table_query)-}}

{% endmacro %}


{% macro empty_column(column_name, data_type) %}

    {%- set dummy_values = elementary.dummy_values() %}

    {%- set column_name = elementary.escape_reserved_keywords(column_name) %}

    {%- if data_type == 'boolean' %}
        cast ({{ dummy_values['boolean'] }} as {{ elementary.edr_type_bool()}}) as {{ column_name }}
    {%- elif data_type == 'timestamp' -%}
        cast('{{ dummy_values['timestamp'] }}' as {{ elementary.edr_type_timestamp() }}) as {{ column_name }}
    {%- elif data_type == 'int' %}
        cast({{ dummy_values['int'] }} as {{ elementary.edr_type_int() }}) as {{ column_name }}
    {%- elif data_type == 'bigint' %}
        cast({{ dummy_values['bigint'] }} as {{ elementary.edr_type_bigint() }}) as {{ column_name }}
    {%- elif data_type == 'float' %}
        cast({{ dummy_values['float'] }} as {{ elementary.edr_type_float() }}) as {{ column_name }}
    {%- elif data_type == 'long_string' %}
        cast('{{ dummy_values['long_string'] }}' as {{ elementary.edr_type_long_string() }}) as {{ column_name }}
    {%- elif data_type == 'nullable(string)' %}
        cast('{{ dummy_values['string'] }}' as Nullable({{ elementary.edr_type_string() }})) as {{ column_name }}
    {%- elif data_type == 'nullable(timestamp)' -%}
        cast('{{ dummy_values['timestamp'] }}' as Nullable({{ elementary.edr_type_timestamp() }})) as {{ column_name }}
    {%- elif data_type == 'nullable(float)' -%}
        cast({{ dummy_values['float'] }} as Nullable({{ elementary.edr_type_float() }})) as {{ column_name }}
    {%- elif data_type == 'nullable(int)' -%}
        cast({{ dummy_values['int'] }} as Nullable({{ elementary.edr_type_int() }})) as {{ column_name }}
    {%- else %}
        cast('{{ dummy_values['string'] }}' as {{ elementary.edr_type_string() }}) as {{ column_name }}
    {%- endif %}

{% endmacro %}


{% macro dummy_values() %}

    {%- set dummy_values = {
     'string': "dummy_string",
     'long_string': "this_is_just_a_long_dummy_string",
     'boolean': 'True',
     'int': 123456789,
     'bigint': 31474836478,
     'float': 123456789.99,
     'timestamp': "2091-02-17"
    } %}

    {{ return(dummy_values) }}

{% endmacro %}
