{% macro monitors_query(thread_number) %}

    {%- set monitored_tables = get_monitored_tables_list(thread_number) %}

    {%- for monitored_table in monitored_tables %}

        {%- set start_msg = 'Started running data monitors on table: ' ~ monitored_table %}
        {%- set end_msg = 'Finished running data monitors on table: ' ~ monitored_table %}

        {% do edr_log(start_msg) %}

        {% do edr_log(end_msg) %}

    {%- endfor %}

    with no_data as (select 1 as num)
    select * from no_data where num = 2

{% endmacro %}
