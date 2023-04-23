{% macro get_columns_from_information_schema(database_name) %}
    {{ return(adapter.dispatch('get_columns_from_information_schema', 'elementary')(database_name)) }}
{% endmacro %}

{# Snowflake, Bigquery#}
{% macro default__get_columns_from_information_schema(database_name) %}
    {% set schema_relation = api.Relation.create(database=database_name).without_identifier() %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ schema_relation.information_schema('COLUMNS') }}

{% endmacro %}

{% macro redshift__get_columns_from_information_schema(database_name) %}
    select
        upper(database_name || '.' || schema_name || '.' || table_name) as full_table_name,
        upper(database_name) as database_name,
        upper(schema_name) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from svv_redshift_columns
        where upper(database_name) = upper('{{ database_name }}')

{% endmacro %}

{% macro postgres__get_columns_from_information_schema(database_name) %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from information_schema.columns

{% endmacro %}