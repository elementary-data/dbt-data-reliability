{% macro get_columns_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = elementary.tuple_to_list_of_size(schema_tuple, 2) %}
    {{ return(adapter.dispatch('get_columns_from_information_schema', 'elementary')(database_name, schema_name)) }}
{% endmacro %}

{# Snowflake, Bigquery#}
{% macro default__get_columns_from_information_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name).without_identifier() %}

    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ schema_relation.information_schema('COLUMNS') }}
    where 1=1
        {%- if schema_name -%} and upper(table_schema) = upper('{{ schema_name }}') {%- endif -%}

{% endmacro %}

{% macro redshift__get_columns_from_information_schema(database_name, schema_name) %}
    select
        upper(database_name || '.' || schema_name || '.' || table_name) as full_table_name,
        upper(database_name) as database_name,
        upper(schema_name) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from svv_redshift_columns
    where upper(database_name) = upper('{{ database_name }}')
        {%- if schema_name -%} and upper(schema_name) = upper('{{ schema_name }}') {%- endif -%}

{% endmacro %}

{% macro postgres__get_columns_from_information_schema(database_name, schema_name) %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from information_schema.columns
    where 1=1
        {%- if schema_name -%} and upper(table_schema) = upper('{{ schema_name }}') {%- endif -%}

{% endmacro %}