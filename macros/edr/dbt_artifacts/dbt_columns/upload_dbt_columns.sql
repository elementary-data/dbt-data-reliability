{% macro upload_dbt_columns() %}
  {% set relation = elementary.get_elementary_relation('dbt_columns') %}
  {% if execute and relation %}
    {% set query %}
      create or replace table {{ relation }} as (
        {{ elementary.get_dbt_columns_query(is_on_run_end=true) }}
      )
    {% endset %}
    {% do run_query(query) %}
  {% endif %}
{% endmacro %}
