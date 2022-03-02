{% macro get_monitored_tables(thread_number) %}
    select *
    from {{ ref('final_tables_config') }}
    where (table_monitored = true or columns_monitored = true)
        and partition_number in ({{ thread_number }})
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})
        and full_table_name is not null
{% endmacro %}


{% macro get_monitored_columns(full_table_name) %}
    select *
    from {{ ref('final_columns_config') }}
    where full_table_name = upper('{{ full_table_name }}')
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_columns_config') }})
        and column_name is not null
{% endmacro %}

-- TODO: should this be here or in configuration?