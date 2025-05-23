{% macro clear_env() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% do elementary_tests.edr_drop_schema(database_name, schema_name) %}
    {% do elementary_tests.edr_drop_schema(elementary.target_database(), generate_schema_name()) %}
{% endmacro %}

{% macro edr_drop_schema(database_name, schema_name) %}
    {% do return(adapter.dispatch('edr_drop_schema', 'elementary_tests')(database_name, schema_name)) %}
{% endmacro %}

{% macro default__edr_drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__edr_drop_schema(database_name, schema_name) %}
    {% do run_query("DROP DATABASE IF EXISTS " ~ schema_name) %}
    {% do adapter.commit() %}
{% endmacro %}
