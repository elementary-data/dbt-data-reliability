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

{% macro athena__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}

    {% set query %}
      merge into {{ target_relation }} as target using {{ tmp_relation }} as src
      ON (target.{{unique_key}} = src.{{ unique_key}})
      when matched
      then update set
      {%- for col in dest_columns %}
          {{ col.column }} = src.{{ col.column }} {{ ", " if not loop.last }}
      {%- endfor %}
      when not matched
        then insert (
        {%- for col in dest_columns %}
            {{ col.column }} {{ ", " if not loop.last }}
        {%- endfor %}

        )
        values (
        {%- for col in dest_columns %}
            src.{{ col.column }} {{ ", " if not loop.last }}
        {%- endfor %}
        )
    {% endset %}
    {% do return(query) %}
{% endmacro %}

{% macro trino__merge_sql(target_relation, tmp_relation, unique_key, dest_columns, incremental_predicates) %}

    {% set query %}
      merge into {{ target_relation }} as target using {{ tmp_relation }} as src
      ON (target.{{unique_key}} = src.{{ unique_key}})
      when matched
      then update set
      {%- for col in dest_columns %}
          {{ col.column }} = src.{{ col.column }} {{ ", " if not loop.last }}
      {%- endfor %}
      when not matched
        then insert (
        {%- for col in dest_columns %}
            {{ col.column }} {{ ", " if not loop.last }}
        {%- endfor %}

        )
        values (
        {%- for col in dest_columns %}
            src.{{ col.column }} {{ ", " if not loop.last }}
        {%- endfor %}
        )
    {% endset %}
    {% do return(query) %}
{% endmacro %}
