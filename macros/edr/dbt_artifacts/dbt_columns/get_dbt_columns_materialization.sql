{% macro get_dbt_columns_materialized() %}
  {% if var("sync", false) %}
    {% do return("table") %}
  {% endif %}
  {% do return(adapter.dispatch("get_dbt_columns_materialized", "elementary")()) %}
{% endmacro %}


{% macro default__get_dbt_columns_materialized() %}
  {% do return("view") %}
{% endmacro %}


{% macro bigquery__get_dbt_columns_materialized() %}
  {% do return("incremental") %}
{% endmacro %}
