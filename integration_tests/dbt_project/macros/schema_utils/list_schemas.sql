{#
  Per-adapter schema listing, scoped to a given database.

  Uses SQL queries instead of adapter.list_schemas() because that method
  is not available in dbt's run-operation context (RuntimeDatabaseWrapper
  does not expose it).
#}

{% macro list_schemas_sql(database) %}
  {% do return(adapter.dispatch('list_schemas_sql', 'elementary_tests')(database)) %}
{% endmacro %}

{% macro default__list_schemas_sql(database) %}
  {% set results = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(catalog_name) = lower('" ~ database ~ "')") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro bigquery__list_schemas_sql(database) %}
  {% set results = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro clickhouse__list_schemas_sql(database) %}
  {% set results = run_query('SHOW DATABASES') %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}
