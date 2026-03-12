{#
  Per-adapter schema existence check, scoped to a given database.

  Uses SQL queries instead of adapter.check_schema_exists() because that
  method is not available in dbt's run-operation context
  (RuntimeDatabaseWrapper does not expose it).
#}
{% macro edr_schema_exists(database, schema_name) %}
    {% do return(
        adapter.dispatch("edr_schema_exists", "elementary_tests")(
            database, schema_name
        )
    ) %}
{% endmacro %}

{% macro default__edr_schema_exists(database, schema_name) %}
    {% set safe_db = database | replace("'", "''") %}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query(
        "SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('"
        ~ safe_db
        ~ "') AND lower(schema_name) = lower('"
        ~ safe_schema
        ~ "')"
    ) %}
    {% do return(result | length > 0) %}
{% endmacro %}

{% macro bigquery__edr_schema_exists(database, schema_name) %}
    {% set safe_db = database | replace("`", "\`") %}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query(
        "SELECT schema_name FROM `"
        ~ safe_db
        ~ "`.INFORMATION_SCHEMA.SCHEMATA WHERE lower(schema_name) = lower('"
        ~ safe_schema
        ~ "')"
    ) %}
    {% do return(result | length > 0) %}
{% endmacro %}

{% macro fabric__edr_schema_exists(database, schema_name) %}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query(
        "SELECT name FROM sys.schemas WHERE lower(name) = lower('"
        ~ safe_schema
        ~ "')"
    ) %}
    {% do return(result | length > 0) %}
{% endmacro %}

{% macro clickhouse__edr_schema_exists(database, schema_name) %}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query(
        "SELECT 1 FROM system.databases WHERE name = '"
        ~ safe_schema
        ~ "' LIMIT 1"
    ) %}
    {% do return(result | length > 0) %}
{% endmacro %}

{% macro spark__edr_schema_exists(database, schema_name) %}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query("SHOW DATABASES LIKE '" ~ safe_schema ~ "'") %}
    {% do return(result | length > 0) %}
{% endmacro %}

{% macro vertica__edr_schema_exists(database, schema_name) %}
    {#- Vertica's v_catalog.schemata is scoped to the current database. -#}
    {% set safe_schema = schema_name | replace("'", "''") %}
    {% set result = run_query(
        "SELECT schema_name FROM v_catalog.schemata WHERE lower(schema_name) = lower('"
        ~ safe_schema
        ~ "')"
    ) %}
    {% do return(result | length > 0) %}
{% endmacro %}
