{% macro get_profile_creation_query(parameters) %}
  {% do return(adapter.dispatch("get_profile_creation_query", "elementary")(parameters)) %}
{% endmacro %}


{% macro snowflake__get_profile_creation_query(parameters) %}
CREATE OR REPLACE USER {{ parameters["user"] }} PASSWORD = '{{ parameters["password"] }}';
CREATE OR REPLACE ROLE {{ parameters["role"] }};
GRANT ROLE {{ parameters["role"] }} TO USER {{ parameters["user"] }};
GRANT USAGE ON WAREHOUSE {{ parameters["warehouse"] }} TO ROLE {{ parameters["role"] }};
GRANT USAGE ON DATABASE {{ parameters["database"] }} TO ROLE {{ parameters["role"] }};
GRANT USAGE ON SCHEMA {{ parameters["database"] }}.{{ parameters["schema"] }} TO ROLE {{ parameters["role"] }};
GRANT SELECT ON ALL TABLES IN SCHEMA {{ parameters["database"] }}.{{ parameters["schema"] }} TO ROLE {{ parameters["role"] }};
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ parameters["database"] }}.{{ parameters["schema"] }} TO ROLE {{ parameters["role"] }};
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ parameters["database"] }}.{{ parameters["schema"] }} TO ROLE {{ parameters["role"] }};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{ parameters["database"] }}.{{ parameters["schema"] }} TO ROLE {{ parameters["role"] }};
{% endmacro %}


{% macro postgres__get_profile_creation_query(parameters) %}
CREATE USER {{ parameters["user"] }} WITH PASSWORD '{{ parameters["password"] }}';
GRANT USAGE ON SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
GRANT SELECT ON ALL TABLES IN SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ parameters["schema"] }} GRANT SELECT ON TABLES TO {{ parameters["user"] }};
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__get_profile_creation_query(parameters) %}
  {% do exceptions.raise_compiler_error('User creation not supported through sql using ' ~ target.type) %}
{% endmacro %}
