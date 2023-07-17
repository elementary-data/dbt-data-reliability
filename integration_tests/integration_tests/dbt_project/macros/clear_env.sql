{% macro clear_env() %}
    {% do elementary.edr_log("Clearing tests environment.") %}
    {% do drop_schema(elementary.target_database(), target.schema) %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% do drop_schema(database_name, schema_name) %}
    {% do elementary.edr_log("Cleared tests environment.") %}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("Dropped schema '{}'.".format(schema_relation | string)) %}
{% endmacro %}
