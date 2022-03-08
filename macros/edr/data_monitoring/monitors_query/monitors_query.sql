{% macro monitors_query(thread_number) %}

    {%- set monitored_tables = get_monitored_tables_list(thread_number) %}

    {%- for monitored_table in monitored_tables %}

        {%- set start_msg = 'Started running data monitors on table: ' ~ monitored_table %}
        {%- set end_msg = 'Finished running data monitors on table: ' ~ monitored_table %}

        {% do edr_log(start_msg) %}

        {%- set table_monitoring_query = elementary.table_monitoring_query(monitored_table) %}
        {%- set column_monitoring_query = elementary.column_monitoring_query(monitored_table) %}

        {%- set insert_table_monitoring = elementary.insert_as_select(this, table_monitoring_query) %}
        {%- do run_query(insert_table_monitoring) %}

        {%- set insert_column_monitoring = elementary.insert_as_select(this, column_monitoring_query) %}
        {%- do run_query(insert_column_monitoring) %}

        {% do edr_log(end_msg) %}

    {%- endfor %}

    select 1 as num
    where num = 2

{% endmacro %}

