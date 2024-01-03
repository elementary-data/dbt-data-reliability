{% macro get_default_table_type() %}
  {% do return(adapter.dispatch("get_default_table_type", "elementary")()) %}
{% endmacro %}

{%- macro athena__get_default_table_type() %}
  {% do return("iceberg") %}
{% endmacro %}

{% macro default__get_default_table_type() %}
  {% do return(none) %}
{% endmacro %}
