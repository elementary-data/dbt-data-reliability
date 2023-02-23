{% macro get_tables_from_information_schema() %}
    {{ return(adapter.dispatch('get_tables_from_information_schema','elementary')()) }}
{% endmacro %}

{# Snowflake, Bigquery #}
{% macro default__get_tables_from_information_schema() %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from {{ schema_relation.information_schema('TABLES') }}

    ),

    information_schema_schemata as (

        select
            upper(catalog_name) as database_name,
            upper(schema_name) as schema_name
        from {{ schema_relation.information_schema('SCHEMATA') }}

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
{% endmacro %}

{% macro redshift__get_tables_from_information_schema() %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from svv_tables

    )

    select
        {{ elementary.full_table_name() }} as full_table_name,
        upper(database_name || '.' || schema_name) as full_schema_name,
        database_name,
        schema_name,
        table_name
    from information_schema_tables
{% endmacro %}

{% macro postgres__get_tables_from_information_schema() %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from information_schema.tables

    )

    select
        {{ elementary.full_table_name() }} as full_table_name,
        upper(database_name || '.' || schema_name) as full_schema_name,
        database_name,
        schema_name,
        table_name
    from information_schema_tables
{% endmacro %}
