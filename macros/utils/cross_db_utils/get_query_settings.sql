{% macro get_query_settings() %}
  {% do return(adapter.dispatch("get_query_settings", "elementary")()) %}
{% endmacro %}

{% macro default__get_query_settings() %}
  {% do return("") %}
{% endmacro %}

{% macro clickhouse__get_query_settings() %}
  {% do return(adapter.get_model_query_settings(model)) %}
{% endmacro %}

