{% macro upload_dbt_columns() %}
  {% set relation = elementary.get_elementary_relation("dbt_columns") %}
  {% if execute and relation %}
    {% set dbt_columns_query = elementary.get_dbt_columns_query(is_model_build_context=false) %}
    {% do elementary.create_or_replace(false, relation, dbt_columns_query) %}
  {% endif %}
{% endmacro %}
