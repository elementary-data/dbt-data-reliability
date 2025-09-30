{% macro get_default_incremental_strategy() %}
  {% do return(adapter.dispatch("get_default_incremental_strategy", "elementary")()) %}
{% endmacro %}

{%- macro athena__get_default_incremental_strategy() %}
  {% do return("merge") %}
{% endmacro %}

{%- macro trino__get_default_incremental_strategy() %}
  {% do return("merge") %}
{% endmacro %}

{%- macro redshift__get_default_incremental_strategy() %}
  {% do return("merge") %}
{% endmacro %}

{% macro default__get_default_incremental_strategy() %}
  {% do return(none) %}
{% endmacro %}
