{% macro clear_tests() %}
    {% if execute %}
        {% do drop_schema(elementary.target_database(), target.schema) %}

        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% do drop_schema(database_name, schema_name) %}

        {% set tests_schema_name = elementary.get_config_var('tests_schema_name') %}
        {% if not tests_schema_name %}
            {{ exceptions.raise_compiler_error('You cannot provide an empty `tests_schema_name` var.') }}
        {% endif %}
        {% set schema_name = schema_name ~ tests_schema_name %}
        {% do drop_schema(database_name, schema_name) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("dropped schema " ~ database_name  ~ "." ~ schema_name) %}
{% endmacro %}

