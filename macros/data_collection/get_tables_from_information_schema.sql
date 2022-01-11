{% macro get_tables_from_information_schema(monitored_db, monitored_schema) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema')(monitored_db, monitored_schema)) }}
{% endmacro %}

{% macro snowflake__get_tables_from_information_schema(monitored_db, monitored_schema) %}

    select
        upper(concat(table_catalog,'.',table_schema,'.',table_name)) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name
    from  {{ monitored_db }}.information_schema.tables
    where table_schema = '{{ monitored_schema }}'

{% endmacro %}
