{#
  Per-adapter schema drop.
#}

{% macro edr_drop_schema(database, schema_name) %}
  {% do return(adapter.dispatch('edr_drop_schema', 'elementary_tests')(database, schema_name)) %}
{% endmacro %}

{% macro default__edr_drop_schema(database, schema_name) %}
  {% set schema_relation = api.Relation.create(database=database, schema=schema_name) %}
  {% do dbt.drop_schema(schema_relation) %}
  {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__edr_drop_schema(database, schema_name) %}
  {% do run_query("DROP DATABASE IF EXISTS `" ~ schema_name ~ "`") %}
  {% do adapter.commit() %}
{% endmacro %}
