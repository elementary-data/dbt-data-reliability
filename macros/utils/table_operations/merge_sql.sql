{% macro merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates=none) -%}
    {{ return(adapter.dispatch('merge_sql', 'elementary')(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates)) }}
{%- endmacro %}

{# Snowflake, Bigquery and Databricks #}
{% macro default__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {% set macro = dbt.get_merge_sql %}
    {% if "incremental_predicates" in macro.get_macro().arguments %}
      {% do return(macro(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates)) %}
    {% endif %}
    {% do return(macro(target_relation, tmp_relation, unique_key, dest_columns)) %}
{% endmacro %}

{% macro postgres__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}
    {% set macro = dbt.get_delete_insert_merge_sql %}
    {% if "incremental_predicates" in macro.get_macro().arguments %}
      {% do return(macro(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates)) %}
    {% endif %}
    {% do return(macro(target_relation, tmp_relation, unique_key, dest_columns)) %}
    {{ return(merge_sql) }}
{% endmacro %}
