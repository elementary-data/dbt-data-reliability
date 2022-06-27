{% macro get_tables_from_information_schema(schema_tuple) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema','elementary')(schema_tuple)) }}
{% endmacro %}

{# Snowflake, Bigquery #}
{% macro default__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    (with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from {{ schema_relation.information_schema('TABLES') }}
        where upper(table_schema) = upper('{{ schema_name }}')

    ),

    information_schema_schemata as (

        select
            upper(catalog_name) as database_name,
            upper(schema_name) as schema_name
        from {{ schema_relation.information_schema('SCHEMATA') }}
        where upper(schema_name) = upper('{{ schema_name }}')

    )

    select
        case when tables.table_name is not null
            then {{ elementary.full_table_name('TABLES') }}
        else null end as full_table_name,
        upper(schemas.database_name || '.' || schemas.schema_name) as full_schema_name,
        schemas.database_name as database_name,
        schemas.schema_name as schema_name,
        tables.table_name
    from information_schema_tables as tables
    full outer join information_schema_schemata as schemas
    on (tables.database_name = schemas.database_name and tables.schema_name = schemas.schema_name)
    )

{% endmacro %}


{% macro redshift__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}

    (with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from svv_tables
            where upper(table_schema) = upper('{{ schema_name }}') and upper(table_catalog) = upper('{{ database_name }}')

    )

    select
        {{ elementary.full_table_name() }} as full_table_name,
        upper(database_name || '.' || schema_name) as full_schema_name,
        database_name,
        schema_name,
        table_name
    from information_schema_tables
    )

{% endmacro %}
