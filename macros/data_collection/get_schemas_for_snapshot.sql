{% macro get_schemas_snapshot_data(monitored_db) %}
    {{ return(adapter.dispatch('get_schemas_snapshot_data')(monitored_db)) }}
{% endmacro %}

{% macro snowflake__get_schemas_snapshot_data(monitored_db) %}
select
    concat(table_catalog,'.',table_schema,'.',table_name) as full_table_name,
    table_catalog as database_name,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable
from  {{ monitored_db }}.information_schema.columns
where table_catalog not in ('snowflake','snowflake_sample_data','util_db')
and table_schema not in ('information_schema')

{% endmacro %}
