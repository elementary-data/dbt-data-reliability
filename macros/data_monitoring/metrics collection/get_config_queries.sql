{% macro monitored_tables(thread_number) %}
    select *
    from {{ ref('final_tables_config') }}
    where (table_monitored = true or columns_monitored = true)
        and thread_number in ({{ thread_number }})
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})
{% endmacro %}


{% macro monitored_columns(full_table_name) %}
    select *
    from {{ ref('final_columns_config') }}
    where full_table_name = upper('{{ full_table_name }}')
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_columns_config') }})
{% endmacro %}