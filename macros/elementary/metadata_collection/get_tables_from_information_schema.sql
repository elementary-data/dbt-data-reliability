{% macro get_tables_from_information_schema(full_schema_name) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema','elementary')(full_schema_name)) }}
{% endmacro %}

{% macro snowflake__get_tables_from_information_schema(full_schema_name) %}
    {% set full_schema_name_split = full_schema_name.split('.') %}
    {% set database_name = full_schema_name_split[0] %}
    {% set schema_name = full_schema_name_split[1] %}

    (with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from  {{ elementary.from_information_schema('TABLES', database_name, schema_name) }}
        where table_schema = upper('{{ schema_name }}')

    ),

    information_schema_schemata as (

        select
            upper(catalog_name) as database_name,
            upper(schema_name) as schema_name
        from  {{ elementary.from_information_schema('SCHEMATA', database_name, schema_name) }}
        where schema_name = upper('{{ schema_name }}')

    )

    select
        case when tables.table_name is not null
            then upper(concat(schemas.database_name,'.',schemas.schema_name,'.',tables.table_name))
        else null end as full_table_name,
        schemas.database_name as database_name,
        schemas.schema_name as schema_name,
        tables.table_name
    from information_schema_tables as tables
    full outer join information_schema_schemata as schemas
    on (tables.database_name = schemas.database_name and tables.schema_name = schemas.schema_name)
    )

{% endmacro %}
