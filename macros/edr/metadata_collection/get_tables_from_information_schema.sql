{% macro get_tables_from_information_schema(full_schema_name) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema','elementary')(full_schema_name)) }}
{% endmacro %}

{# Snowflake, Bigquery, Redshift #}
{% macro default__get_tables_from_information_schema(full_schema_name) %}
    {% set full_schema_name_split = full_schema_name.split('.') %}
    {% set database_name = full_schema_name_split[0] %}
    {% set database_name_string = database_name | replace('"','') %}
    {% set schema_name = full_schema_name_split[1] %}
    {% set schema_name_string = schema_name | replace('"','') | upper %}

    (with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from  {{ elementary.from_information_schema('TABLES', database_name, schema_name) }}
        where upper(table_schema) = upper('{{ schema_name_string }}')

    ),

    information_schema_schemata as (

        select
            upper(catalog_name) as database_name,
            upper(schema_name) as schema_name
        from  {{ elementary.from_information_schema('SCHEMATA', database_name) }}
        where upper(schema_name) = upper('{{ schema_name_string }}')

        {# Public is missing from schemata in Redshift #}
        {%- if schema_name_string == 'PUBLIC' %}
            union all
                select '{{ database_name_string }}' as database_name, 'PUBLIC' as schema_name
        {%- endif %}

    )

    select
        case when tables.table_name is not null
            then {{ elementary.full_table_name('tables') }}
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
