{% macro get_monitored_tables_list(thread_number) %}
    {%- set query %}
        select full_table_name
        from {{ ref('final_tables_config') }}
            where (table_monitored = true or columns_monitored = true)
            and partition_number in ({{ thread_number }})
            and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})
            and full_table_name is not null
    {%- endset %}
    {%- set tables_list = elementary.result_column_to_list(query) %}
    {{ return(tables_list) }}
{% endmacro %}