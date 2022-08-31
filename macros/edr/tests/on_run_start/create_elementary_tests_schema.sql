{% macro create_elementary_tests_schema() %}
    {% if execute and flags.WHICH in ['test', 'build'] %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_config_var('tests_schema_name') %}
        {% if not tests_schema_name %}
            {{ exceptions.raise_compiler_error('You cannot provide an empty `tests_schema_name` var.') }}
        {% endif %}
        {% set schema_name = schema_name ~ tests_schema_name %}
        {%- if not adapter.check_schema_exists(database_name, schema_name) %}
            {{ elementary.edr_log("Creating Elementary's tests schema.") }}
            {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}
            {%- do dbt.create_schema(schema_relation) %}
            {% do adapter.commit() %}
        {%- endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}