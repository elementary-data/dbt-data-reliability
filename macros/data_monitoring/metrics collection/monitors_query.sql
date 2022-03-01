{% macro monitors_query(thread_number) %}
    -- depends_on: {{ ref('elementary_runs') }}
    -- depends_on: {{ ref('final_tables_config') }}
    -- depends_on: {{ ref('final_columns_config') }}
    -- depends_on: {{ ref('final_should_backfill') }}
    -- depends_on: {{ ref('temp_monitoring_metrics') }}

    {%- set monitored_tables = run_query(monitored_tables(thread_number)) %}
    {%- if execute %}
        {%- set table_config_column_names = monitored_tables.column_names %}
    {%- endif %}

    {%- for monitored_table in monitored_tables %}
        {%- set full_table_name = monitored_table[table_config_column_names[1]] %}
        {%- set database_name = monitored_table[table_config_column_names[2]] %}
        {%- set schema_name = monitored_table[table_config_column_names[3]] %}
        {%- set table_name = monitored_table[table_config_column_names[4]] %}
        {%- set timestamp_column = monitored_table[table_config_column_names[5]] %}
        {%- set bucket_duration_hours = monitored_table[table_config_column_names[6]] | int %}
        {%- set table_monitored = monitored_table[table_config_column_names[7]] %}
        {%- set columns_monitored = monitored_table[table_config_column_names[9]] %}
        {%- set table_should_backfill = monitored_table[table_config_column_names[10]] %}
        {%- set timestamp_column_data_type = monitored_table[table_config_column_names[11]] %}

        {%- if table_monitored is sameas true %}
            {%- if monitored_table[table_config_column_names[8]] %}
                {%- set config_table_monitors = fromjson(monitored_table[table_config_column_names[8]]) %}
            {%- endif %}
            {%- set table_monitors = elementary.get_table_monitors(config_table_monitors) %}
        {%- endif %}

        {%- if columns_monitored is sameas true %}
            {%- set column_monitors_config = elementary.get_columns_monitors_config(full_table_name) %}
        {%- endif %}

        {%- set should_backfill_query %}
            select should_backfill
            from {{ ref('final_should_backfill') }}
            where full_table_name = '{{ full_table_name }}'
        {%- endset %}
        {%- set should_backfill = elementary.result_value(should_backfill_query) %}

        {%- set start_msg = 'Started running data monitors on table: ' ~ full_table_name %}
        {%- set end_msg = 'Finished running data monitors on table: ' ~ full_table_name %}
        {% do elementary.edr_log(start_msg) %}
        {% do elementary.table_monitors_query(full_table_name, timestamp_column, var('days_back'), bucket_duration_hours, table_monitors, column_monitors_config, should_backfill, timestamp_column_data_type) %}
        {% do elementary.edr_log(end_msg) %}
    {%- endfor %}

    select 1 as num
    where num = 2

{% endmacro %}

