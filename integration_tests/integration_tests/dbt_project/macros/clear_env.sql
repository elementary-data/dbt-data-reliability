{% macro clear_env() %}
    {% do drop_schema(elementary.target_database(), target.schema) %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% do drop_schema(database_name, schema_name) %}
    {% do drop_schema(elementary.target_database(), "test_seeds") %}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}
