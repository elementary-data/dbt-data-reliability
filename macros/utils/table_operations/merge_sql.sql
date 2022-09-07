{% macro merge_sql(target_relation, tmp_relation, unique_key, dest_columns) -%}
    {{ return(adapter.dispatch('merge_sql', 'elementary')(target_relation, tmp_relation, unique_key, dest_columns)) }}
{%- endmacro %}

{# Snowflake, Bigquery and Databricks #}
{% macro default__merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
    {%- set merge_sql = dbt.get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
    {{ return(merge_sql) }}
{% endmacro %}

{% macro redshift__merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
    {%- set merge_sql = dbt.get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
    {{ return(merge_sql) }}
{% endmacro %}
