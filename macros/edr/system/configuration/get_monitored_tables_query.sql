{% macro get_monitored_tables_query(thread_number) %}
    select *
    from {{ ref('final_tables_config') }}
    where (table_monitored = true or columns_monitored = true)
        and partition_number in ({{ thread_number }})
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})
        and full_table_name is not null
{% endmacro %}

{% macro get_monitored_table_config_query(full_table_name) %}
    select *
    from {{ ref('final_tables_config') }}
        where upper(full_table_name) = upper('{{ full_table_name }}')
    order by config_loaded_at desc
    limit 1
{% endmacro %}
