{% macro get_columns_from_information_schema(schema_tuple) %}
    {{ return(adapter.dispatch('get_columns_from_information_schema', 'elementary')(schema_tuple)) }}
{% endmacro %}

{# Snowflake, Bigquery#}
{% macro default__get_columns_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ schema_relation.information_schema('COLUMNS') }}
    where upper(table_schema) = upper('{{ schema_name }}')

{% endmacro %}


{% macro redshift__get_columns_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from svv_columns
        where upper(table_schema) = upper('{{ schema_name }}')

{% endmacro %}