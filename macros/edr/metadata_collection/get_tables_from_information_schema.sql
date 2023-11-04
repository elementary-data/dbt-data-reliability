{% macro get_tables_from_information_schema(schema_tuple) %}
    {{ return(adapter.dispatch('get_tables_from_information_schema','elementary')(schema_tuple)) }}
{% endmacro %}

{# Snowflake, Bigquery #}
{% macro default__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    with information_schema_tables as (

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
{% endmacro %}

{% macro redshift__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from pg_catalog.svv_tables
            where upper(table_schema) = upper('{{ schema_name }}') and upper(table_catalog) = upper('{{ database_name }}')

    )

    select
        {{ elementary.full_table_name() }} as full_table_name,
        upper(database_name || '.' || schema_name) as full_schema_name,
        database_name,
        schema_name,
        table_name
    from information_schema_tables
{% endmacro %}

{% macro postgres__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}

    with information_schema_tables as (

        select
            upper(table_catalog) as database_name,
            upper(table_schema) as schema_name,
            upper(table_name) as table_name
        from information_schema.tables
            where upper(table_schema) = upper('{{ schema_name }}') and upper(table_catalog) = upper('{{ database_name }}')

    )

    select
        {{ elementary.full_table_name() }} as full_table_name,
        upper(database_name || '.' || schema_name) as full_schema_name,
        database_name,
        schema_name,
        table_name
    from information_schema_tables
{% endmacro %}

{% macro databricks__get_tables_from_information_schema(schema_tuple) %}
    {%- set database_name, schema_name = schema_tuple %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    {#
        Database only exists when using databricks with catalog (it is the catalog).
        When using databricks without catalog, it is none.
    #}
    {% set is_catalog = schema_relation.database is not none %}

    {# Information schema only exists on databricks with catalog #}
    {% if is_catalog %}
        with information_schema_tables as (

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
    {% else %}
        {{ elementary.get_empty_tables_from_information_schema_table() }}
    {% endif %}
{% endmacro %}

{% macro get_empty_tables_from_information_schema_table() %}
    {{ elementary.empty_table([
        ('full_table_name', 'string'),
        ('full_schema_name', 'string'),
        ('database_name', 'string'),
        ('schema_name', 'string'),
        ('table_name', 'string'),
    ]) }}
{% endmacro %}
