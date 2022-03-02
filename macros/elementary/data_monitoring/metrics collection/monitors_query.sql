{% macro monitors_query(thread_number) %}
    -- depends_on: {{ ref('elementary_runs') }}
    -- depends_on: {{ ref('final_tables_config') }}
    -- depends_on: {{ ref('final_columns_config') }}
    -- depends_on: {{ ref('final_should_backfill') }}
    -- depends_on: {{ ref('temp_monitoring_metrics') }}

    {%- set monitored_tables = run_query(elementary.get_monitored_tables_query(thread_number)) %}

    {%- for monitored_table in monitored_tables %}
        {%- set full_table_name = elementary.insensitive_get_dict_value(monitored_table, 'full_table_name') %}
        {%- set timestamp_column = elementary.insensitive_get_dict_value(monitored_table, 'timestamp_column') %}
        {%- set bucket_duration_hours = elementary.insensitive_get_dict_value(monitored_table, 'bucket_duration_hours') | int %}
        {%- set table_monitored = elementary.insensitive_get_dict_value(monitored_table, 'table_monitored') %}
        {%- set columns_monitored = elementary.insensitive_get_dict_value(monitored_table, 'columns_monitored') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(monitored_table, 'timestamp_column_data_type') %}
        {%- set table_monitors_str = elementary.insensitive_get_dict_value(monitored_table, 'table_monitors') %}

        {% set table_monitors = none %}
        {%- if table_monitored is sameas true %}
            {%- set default_monitors = elementary.get_default_table_monitors() %}
            {%- if table_monitors_str is not none %}
                {%- set configured_table_monitors = fromjson(table_monitors_str) %}
            {%- endif %}
            {%- if configured_table_monitors is defined and configured_table_monitors is not none and configured_table_monitors | length > 0 %}
                {%- set table_monitors = configured_table_monitors %}
            {%- else %}
                {%- set table_monitors = default_table_monitors %}
            {%- endif %}
            {# schema_changes is a different flow #}
            {% if 'schema_changes' in monitors_list %}
                {%- do table_monitors.remove('schema_changes') %}
            {% endif %}
        {%- endif %}

        {% set column_monitors = none %}
        {%- if columns_monitored is sameas true %}
            {%- set column_monitors = elementary.get_columns_monitors_config(full_table_name) %}
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
        {% do elementary.table_monitors_query(full_table_name, timestamp_column, var('days_back'), bucket_duration_hours, table_monitors, column_monitors, should_backfill, timestamp_column_data_type) %}
        {% do elementary.edr_log(end_msg) %}
    {%- endfor %}

    select 1 as num
    where num = 2

{% endmacro %}

