{% macro get_information_schema_columns_materialized() %}
  {% if var("sync", false) %}
    {% do return("table") %}
  {% endif %}
  {% do return("view") %}
{% endmacro %}

