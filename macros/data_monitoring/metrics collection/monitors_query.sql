{% macro monitors_query(thread_number) %}
    {%- set monitored_tables = run_query(monitored_tables(thread_number)) %}
    {%- for monitored_table in monitored_tables %}
        {%- set aaa = monitored_table['full_table_name'] %}
        {{ return(aaa) }}
    {%- endfor %}

{% endmacro %}

