{% macro get_column_size() %}
  {{ return(adapter.dispatch('get_column_size', 'elementary')()) }}
{% endmacro %}

{% macro default__get_column_size() %}
  {{ return(65535) }}
{% endmacro %}

{% macro snowflake__get_column_size() %}
  {{ return(16777216) }}
{% endmacro %}

{% macro bigquery__get_column_size() %}
  {{ return(10485760) }}
{% endmacro %}
