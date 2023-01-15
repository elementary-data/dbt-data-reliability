{% macro merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates=none) -%}
    {{ return(adapter.dispatch('merge_sql', 'elementary')(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates)) }}
{%- endmacro %}

{# Snowflake, Bigquery and Databricks #}
{% macro default__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {%- set merge_sql = dbt.get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {{ return(merge_sql) }}
{% endmacro %}

{% macro postgres__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {%- set merge_sql = dbt.get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {{ return(merge_sql) }}
{% endmacro %}
