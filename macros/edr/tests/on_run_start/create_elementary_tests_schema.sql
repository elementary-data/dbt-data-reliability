{% macro create_elementary_tests_schema() %}
    {% if execute and flags.WHICH == 'test' %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ '__tests' %}
        {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}
        {%- if not adapter.check_schema_exists(database_name, schema_name) %}
            {{ elementary.debug_log('schema ' ~ database_name ~ '.' ~ schema_name ~ ' doesnt exist, creating it') }}
            {%- do dbt.create_schema(schema_relation) %}
            {% do adapter.commit() %}
        {%- endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}