{% macro monitors_query(thread_number) %}
    -- depends_on: {{ ref('elementary_runs') }}
    -- depends_on: {{ ref('final_tables_config') }}
    -- depends_on: {{ ref('final_columns_config') }}
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
            {%- set table_monitors = get_table_monitors(config_table_monitors) %}
        {%- endif %}

        {%- if columns_monitored is sameas true %}
            {%- set column_monitors_config = get_columns_monitors_config(full_table_name) %}
        {%- endif %}

        --TODO: for columns - one of them could be with should_backfill=True and the rest will be False
        {%- set should_backfill = false %}
        {%- if table_should_backfill is sameas true %}
            {%- set should_backfill = true %}
        {%- elif column_monitors_config is sequence and column_monitors_config| length > 0 %}
            {%- set column_monitor_config = column_monitors_config[0] %}
            {%- if column_monitor_config is mapping %}
                {%- if column_monitor_config.get('should_backfill') is sameas true %}
                    {%- set should_backfill = true %}
                {% endif %}
            {%- endif %}
        {%- endif %}

        {%- set start_msg = 'Started running data monitors on table: ' ~ full_table_name %}
        {%- set end_msg = 'Finished running data monitors on table: ' ~ full_table_name %}
        {% do edr_log(start_msg) %}
        {% do table_monitors_query(full_table_name, timestamp_column, var('days_back'), bucket_duration_hours, table_monitors, column_monitors_config, should_backfill, timestamp_column_data_type) %}
        {% do edr_log(end_msg) %}
    {%- endfor %}

    select 1 as num
    where num = 2

{% endmacro %}

