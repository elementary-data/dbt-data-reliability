{% macro create_user(username, password) %}
  {% do return(adapter.dispatch("create_user", "elementary")(username, password)) %}
{% endmacro %}


{% macro snowflake__create_user(username, password) %}
  CREATE USER {{ username }} 
  PASSWORD = '{{ password }}';
{% endmacro %}


{% macro postgres__create_user(username, password) %}
  CREATE USER {{ username }} 
  WITH PASSWORD '{{ password }}';
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__create_user(username, password) %}
  {% do exceptions.raise_compiler_error('User creation not supported through sql using ' ~ target.type) %}
{% endmacro %}
