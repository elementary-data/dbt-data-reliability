{% macro table_exists_in_target(table_name) %}

    {%- set source_relation = adapter.get_relation(
          database=target_database(),
          schema=target.schema,
          identifier=table_name) -%}

    {%- set table_exists = source_relation is not none -%}
    {{ return(table_exists) }}

{% endmacro %}