{#
  Per-adapter schema listing, scoped to a given database.

  Uses SQL queries instead of adapter.list_schemas() because that method
  is not available in dbt's run-operation context (RuntimeDatabaseWrapper
  does not expose it).
#}

{% macro edr_list_schemas(database) %}
  {% do return(adapter.dispatch('edr_list_schemas', 'elementary_tests')(database)) %}
{% endmacro %}

{% macro default__edr_list_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('" ~ database ~ "')") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro bigquery__edr_list_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro clickhouse__edr_list_schemas(database) %}
  {% set results = run_query('SHOW DATABASES') %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}
