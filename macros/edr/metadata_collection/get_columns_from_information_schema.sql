{% macro get_columns_from_information_schema(schema_tuple, table_name = none) %}
    {%- set database_name, schema_name = schema_tuple %}
    {{ return(adapter.dispatch('get_columns_from_information_schema', 'elementary')(database_name, schema_name, table_name)) }}
{% endmacro %}

{# Snowflake #}
{% macro default__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}
    {% set column_relation = schema_relation.information_schema('COLUMNS') %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ column_relation }}
    where upper(table_schema) = upper('{{ schema_name }}')
    {% if table_name %}
      and upper(table_name) = upper('{{ table_name }}')
    {% endif %}
{% endmacro %}

{% macro bigquery__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}
    {% set columns_schema = schema_relation.information_schema('COLUMNS') %}
    {% if elementary.can_query_relation(columns_schema) %}
      {{ elementary.default__get_columns_from_information_schema(database_name, schema_name, table_name) }}
    {% else %}
      {{ elementary.get_empty_columns_from_information_schema_table() }}
    {% endif %}
{% endmacro %}

{% macro redshift__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from pg_catalog.svv_columns
    where upper(table_schema) = upper('{{ schema_name }}')
    {% if table_name %}
      and upper(table_name) = upper('{{ table_name }}')
    {% endif %}
{% endmacro %}

{% macro postgres__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from information_schema.columns
    where upper(table_schema) = upper('{{ schema_name }}')
    {% if table_name %}
      and upper(table_name) = upper('{{ table_name }}')
    {% endif %}
{% endmacro %}

{% macro databricks__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {% if target.catalog is none or target.catalog.lower() == 'hive_metastore' %}
        {# Information schema is only available when using Unity Catalog. #}
        {% do return(elementary.get_empty_columns_from_information_schema_table()) %}
    {% endif %}
    {% set column_relation = api.Relation.create('system', 'information_schema', 'columns') %}
    select
        upper(table_catalog || '.' || table_schema || '.' || table_name) as full_table_name,
        upper(table_catalog) as database_name,
        upper(table_schema) as schema_name,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        data_type
    from {{ column_relation }}
    where upper(table_schema) = upper('{{ schema_name }}')
    {% if table_name %}
        and upper(table_name) = upper('{{ table_name }}')
    {% endif %}
{% endmacro %}

{% macro spark__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {{ elementary.get_empty_columns_from_information_schema_table() }}
{% endmacro %}

{% macro get_empty_columns_from_information_schema_table() %}
    {{ elementary.empty_table([('full_table_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('data_type', 'string')]) }}
{% endmacro %}

{% macro athena__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {{ elementary.empty_table([('full_table_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('data_type', 'string')]) }}
{% endmacro %}

{% macro trino__get_columns_from_information_schema(database_name, schema_name, table_name = none) %}
    {{ elementary.get_empty_columns_from_information_schema_table() }}
{% endmacro %}
