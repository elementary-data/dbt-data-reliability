{% macro create_elementary_tests_schema() %}
    {% if execute and elementary.is_test_command() %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}
        {%- if tests_schema_name != schema_name and not adapter.check_schema_exists(database_name, tests_schema_name) %}
            {{ elementary.edr_log("Creating Elementary's tests schema.") }}
            {% set schema_relation = api.Relation.create(database=database_name, schema=tests_schema_name).without_identifier() %}
            {%- do dbt.create_schema(schema_relation) %}
            {% do adapter.commit() %}
        {%- endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}