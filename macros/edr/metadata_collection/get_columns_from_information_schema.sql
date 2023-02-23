{% macro get_columns_from_information_schema() %}
    {{ return(adapter.dispatch('get_columns_from_information_schema', 'elementary')()) }}
{% endmacro %}

{# Snowflake, Bigquery#}
{% macro default__get_columns_from_information_schema() %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ schema_relation.information_schema('COLUMNS') }}

{% endmacro %}

{% macro redshift__get_columns_from_information_schema() %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from svv_columns

{% endmacro %}

{% macro postgres__get_columns_from_information_schema() %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from information_schema.columns

{% endmacro %}