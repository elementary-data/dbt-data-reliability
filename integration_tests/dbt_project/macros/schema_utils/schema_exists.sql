{#
  Per-adapter schema existence check, scoped to a given database.

  Uses SQL queries instead of adapter.check_schema_exists() because that
  method is not available in dbt's run-operation context
  (RuntimeDatabaseWrapper does not expose it).
#}

{% macro schema_exists_sql(database, schema_name) %}
  {% do return(adapter.dispatch('schema_exists_sql', 'elementary_tests')(database, schema_name)) %}
{% endmacro %}

{% macro default__schema_exists_sql(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('" ~ database ~ "') AND lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro bigquery__schema_exists_sql(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA WHERE lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro clickhouse__schema_exists_sql(database, schema_name) %}
  {% set result = run_query("SELECT 1 FROM system.databases WHERE name = '" ~ schema_name ~ "' LIMIT 1") %}
  {% do return(result | length > 0) %}
{% endmacro %}
