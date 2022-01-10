{% macro get_schemas_snapshot_data(monitored_db) %}
    {{ return(adapter.dispatch('get_schemas_snapshot_data')(monitored_db)) }}
{% endmacro %}

{% macro snowflake__get_schemas_snapshot_data(monitored_db) %}

    select
        upper(concat(table_catalog,'.',table_schema,'.',table_name)) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from  {{ monitored_db }}.information_schema.columns
    where table_catalog not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA','UTIL_DB')
    and table_schema not in ('INFORMATION_SCHEMA')

{% endmacro %}
