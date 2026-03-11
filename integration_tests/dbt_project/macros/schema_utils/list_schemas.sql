{#
  Per-adapter schema listing, scoped to a given database.

  Uses SQL queries instead of adapter.list_schemas() because that method
  is not available in dbt's run-operation context (RuntimeDatabaseWrapper
  does not expose it).
#}
{% macro edr_list_schemas(database) %}
    {% do return(adapter.dispatch("edr_list_schemas", "elementary_tests")(database)) %}
{% endmacro %}

{% macro default__edr_list_schemas(database) %}
    {% set safe_db = database | replace("'", "''") %}
    {% set results = run_query(
        "SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('"
        ~ safe_db
        ~ "')"
    ) %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}

{% macro bigquery__edr_list_schemas(database) %}
    {% set safe_db = database | replace("`", "\`") %}
    {% set results = run_query(
        "SELECT schema_name FROM `"
        ~ safe_db
        ~ "`.INFORMATION_SCHEMA.SCHEMATA"
    ) %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}

{% macro fabric__edr_list_schemas(database) %}
    {# Fabric does not support information_schema.schemata; use sys.schemas instead #}
    {% set results = run_query("SELECT name FROM sys.schemas") %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}

{% macro clickhouse__edr_list_schemas(database) %}
    {% set results = run_query("SHOW DATABASES") %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}

{% macro spark__edr_list_schemas(database) %}
    {% set results = run_query("SHOW DATABASES") %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}

{% macro vertica__edr_list_schemas(database) %}
    {#- Vertica's v_catalog.schemata is scoped to the current database and
        does not have a database_name filter column. -#}
    {% set results = run_query("SELECT schema_name FROM v_catalog.schemata") %}
    {% set schemas = [] %}
    {% for row in results %} {% do schemas.append(row[0]) %} {% endfor %}
    {% do return(schemas) %}
{% endmacro %}
