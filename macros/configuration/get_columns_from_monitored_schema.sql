{% macro get_columns_from_monitored_schema(monitored_db, monitored_schema) %}
    {{ return(adapter.dispatch('get_columns_from_monitored_schema')(monitored_db, monitored_schema)) }}
{% endmacro %}

{% macro snowflake__get_columns_from_monitored_schema(monitored_db, monitored_schema) %}

    select
        upper(concat(table_catalog,'.',table_schema,'.',table_name)) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name
    from  {{ monitored_db }}.information_schema.columns
    where table_catalog = '{{ monitored_db }}'
    and table_schema = '{{ monitored_schema }}'

{% endmacro %}
