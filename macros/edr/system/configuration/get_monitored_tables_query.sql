{% macro get_monitored_table_config_query(full_table_name) %}
    select *
    from {{ ref('final_tables_config') }}
        where upper(full_table_name) = upper('{{ full_table_name }}')
    order by config_loaded_at desc
    limit 1
{% endmacro %}
