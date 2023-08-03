{% macro upload_dbt_columns() %}
  {% set relation = elementary.get_elementary_relation("dbt_columns") %}
  {% if execute and relation %}
    {% set query %}
      create or replace table {{ relation }} as (
        {{ elementary.get_dbt_columns_query(in_model_build_context=false) }}
      )
    {% endset %}
    {% do run_query(query) %}
  {% endif %}
{% endmacro %}
