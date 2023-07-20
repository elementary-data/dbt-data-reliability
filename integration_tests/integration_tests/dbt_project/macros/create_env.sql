{% macro create_env() %}
    {% do elementary_tests.edr_create_schema(elementary.target_database(), "test_seeds") %}
{% endmacro %}

{% macro edr_create_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.create_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}
