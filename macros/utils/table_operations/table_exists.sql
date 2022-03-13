{% macro table_exists_in_target(table_name, schema_name, database_name) %}

{# dbt models can be found with identifier only #}
{# for non-dbt tables database_name and schema_name are required #}

    {%- if not database_name is defined %}
        {%- set database_name = elementary.target_database() %}
    {%- endif %}
    {%- if not schema_name is defined %}
        {%- set schema_name = target.schema %}
    {%- endif %}

    {%- set source_relation = adapter.get_relation(
          database=database_name,
          schema=schema_name,
          identifier=table_name) -%}

    {%- set table_exists = source_relation is not none -%}
    {{ return(table_exists) }}

{% endmacro %}