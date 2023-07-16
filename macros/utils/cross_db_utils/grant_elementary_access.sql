{% macro grant_elementary_access(username, role=none) %}
  {% if not role %}
    {% set role = "ELEMENTARY_ROLE" %}
  {% endif %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% if elementary_database is none %}
    {% do exceptions.raise_compiler_error("Elementary schema not found") %}
  {% endif %}
  {% do return(adapter.dispatch("grant_elementary_access", "elementary")(username, elementary_database, elementary_schema, role)) %}
{% endmacro %}


{% macro snowflake__grant_elementary_access(username, database, schema, role) %}
  CREATE OR REPLACE ROLE {{ role }};
  GRANT ROLE {{ role }} TO USER {{ username }};
  GRANT USAGE ON WAREHOUSE {{ target.warehouse }} TO ROLE {{ role }};
  GRANT USAGE ON DATABASE {{ database }} TO ROLE {{ role }};
  GRANT USAGE ON SCHEMA {{ database }}.{{ schema }} TO ROLE {{ role }};
  GRANT SELECT ON ALL TABLES IN SCHEMA {{ database }}.{{ schema }} TO ROLE {{ role }};
{% endmacro %}


{% macro postgres__grant_elementary_access(username, database, schema, role) %}
  GRANT USAGE ON SCHEMA {{ schema }} TO {{ username }};
  GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ username }};
{% endmacro %}


{% macro redshift__grant_elementary_access(username, database, schema, role) %}
  GRANT SELECT
  ON ALL TABLES IN SCHEMA {{ database }}.{{ schema }}
  TO {{ username }};
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__grant_elementary_access(username, database, schema, role) %}
  {% do exceptions.raise_compiler_error('Access control not supported through sql using ' ~ target.type) %}
{% endmacro %}
