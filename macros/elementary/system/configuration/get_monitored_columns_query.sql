{% macro get_monitored_columns_query(full_table_name) %}
    select *
    from {{ ref('final_columns_config') }}
        where full_table_name = upper('{{ full_table_name }}')
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_columns_config') }})
        and column_name is not null
{% endmacro %}
