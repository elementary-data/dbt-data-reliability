{% macro get_information_schema_columns_materialized() %}
  {% if var("sync", false) %}
    {% do return("table") %}
  {% endif %}
  {% do return(adapter.dispatch("get_information_schema_columns_materialized", "elementary")()) %}
{% endmacro %}


{% macro default__get_information_schema_columns_materialized() %}
  {% do return("view") %}
{% endmacro %}


{% macro bigquery__get_information_schema_columns_materialized() %}
  {% do return("incremental") %}
{% endmacro %}
