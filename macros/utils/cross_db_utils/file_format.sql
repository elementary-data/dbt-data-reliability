{% macro get_default_file_format() %}
  {% do return(adapter.dispatch("get_default_file_format", "elementary")()) %}
{% endmacro %}

{%- macro glue__get_default_file_format() %}
  {% do return("delta") %}
{% endmacro %}

{% macro default__get_default_file_format() %}
  {% do return(none) %}
{% endmacro %}
