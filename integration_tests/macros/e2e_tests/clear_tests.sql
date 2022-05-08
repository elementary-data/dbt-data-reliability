{% macro clear_tests() %}
    {% if execute %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% do drop_schema(database_name, schema_name) %}
        {% set schema_name = schema_name ~ '__tests' %}
        {% do drop_schema(database_name, schema_name) %}
        {% do drop_schema(elementary.target_database(), target.schema) %}
        {% do drop_schema(var('dbt_artifacts_database', elementary.target_database()), var('dbt_artifacts_schema', target.schema)) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("dropped schema " ~ database_name  ~ "." ~ schema_name) %}
{% endmacro %}

