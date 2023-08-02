{% macro get_dbt_columns_materialization() %}
  {% if var("sync", false) %}
    {% do return("table") %}
  {% endif %}
  {% do return(adapter.dispatch("get_dbt_columns_materialization", "elementary")()) %}
{% endmacro %}


{% macro default__get_dbt_columns_materialization() %}
  {% do return("view") %}
{% endmacro %}


{% macro bigquery__get_dbt_columns_materialization() %}
  {% do return("table") %}
{% endmacro %}
