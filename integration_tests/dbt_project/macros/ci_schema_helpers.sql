{#
  Helper macros for CI schema operations.

  These use SQL queries instead of adapter methods because
  adapter.list_schemas() and adapter.check_schema_exists() are not
  available in dbt's run-operation context (RuntimeDatabaseWrapper
  does not expose them).

  All queries are scoped to the CI database (from the dbt profile)
  for safety.
#}

{# ── Per-adapter schema listing (scoped to CI database) ────────────── #}

{% macro list_ci_schemas(database) %}
  {% do return(adapter.dispatch('list_ci_schemas', 'elementary_tests')(database)) %}
{% endmacro %}

{% macro default__list_ci_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('" ~ database ~ "')") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro bigquery__list_ci_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro clickhouse__list_ci_schemas(database) %}
  {% set results = run_query('SHOW DATABASES') %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}


{# ── Per-adapter schema existence check (scoped to CI database) ────── #}

{% macro ci_schema_exists(database, schema_name) %}
  {% do return(adapter.dispatch('ci_schema_exists', 'elementary_tests')(database, schema_name)) %}
{% endmacro %}

{% macro default__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('" ~ database ~ "') AND lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro bigquery__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA WHERE lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro clickhouse__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT 1 FROM system.databases WHERE name = '" ~ schema_name ~ "' LIMIT 1") %}
  {% do return(result | length > 0) %}
{% endmacro %}
