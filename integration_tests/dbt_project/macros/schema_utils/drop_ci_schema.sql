{#
  Per-adapter schema drop for CI cleanup.
#}

{% macro drop_ci_schema(database, schema_name) %}
  {% do return(adapter.dispatch('drop_ci_schema', 'elementary_tests')(database, schema_name)) %}
{% endmacro %}

{% macro default__drop_ci_schema(database, schema_name) %}
  {% set schema_relation = api.Relation.create(database=database, schema=schema_name) %}
  {% do dbt.drop_schema(schema_relation) %}
  {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__drop_ci_schema(database, schema_name) %}
  {% do run_query("DROP DATABASE IF EXISTS `" ~ schema_name ~ "`") %}
  {% do adapter.commit() %}
{% endmacro %}
