{% macro get_tables_from_information_schema(database_name, schema_name) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema')(database_name, schema_name)) }}
{% endmacro %}

{% macro snowflake__get_tables_from_information_schema(database_name, schema_name) %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from  {{ database_name }}.information_schema.tables
        where upper(table_schema) = upper('{{ schema_name }}')

    ),

    information_schema_schemata as (

        select
            upper(catalog_name) as database_name,
            upper(schema_name) as schema_name,
            null as table_name
        from  {{ database_name }}.information_schema.schemata
        where upper(schema_name) = upper('{{ schema_name }}')

    )

    select
        coalesce(tables.database_name, schemas.database_name) as database_name,
        coalesce(tables.schema_name, schemas.schema_name) as schema_name,
        tables.table_name
    from information_schema_tables as tables
    full outer join information_schema_schemata as schemas
    on (tables.database_name = schemas.database_name and tables.schema_name = schemas.schema_name)

{% endmacro %}
