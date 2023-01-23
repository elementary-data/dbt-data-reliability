{% macro long_string_size() %}
  {{ return(adapter.dispatch('long_string_size', 'elementary')()) }}
{% endmacro %}

{% macro default__long_string_size() %}
  {{ return(elementary.get_config_var('long_string_size')) }}
{% endmacro %}

{% macro postgres__long_string_size() %}
  {{ return(none) }}
{% endmacro %}

{% macro redshift__long_string_size() %}
  {{ return(elementary.get_config_var('long_string_size')) }}
{% endmacro %}

{% macro snowflake__long_string_size() %}
  {{ return(16777216) }}
{% endmacro %}

{% macro bigquery__long_string_size() %}
  {{ return(10485760) }}
{% endmacro %}
