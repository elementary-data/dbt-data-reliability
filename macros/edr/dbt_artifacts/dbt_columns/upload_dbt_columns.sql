{% macro upload_dbt_columns() %}
  {% if elementary.get_dbt_columns_materialization() == "view" %}
    {% do return() %}
  {% endif %}
  {% set relation = elementary.get_elementary_relation('dbt_columns') %}
  {% call statement('upload_dbt_columns', fetch_result=False) %}
    create or replace table {{ relation }} as (
      {{ elementary.get_dbt_columns_query() }}
    )
  {% endcall %}
  {% do adapter.commit() %}
{% endmacro %}
