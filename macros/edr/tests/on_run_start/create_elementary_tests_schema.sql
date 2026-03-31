{% macro create_elementary_tests_schema() %}
    {% if execute and elementary.is_test_command() %}
        {% set database_name, schema_name = (
            elementary.get_package_database_and_schema("elementary")
        ) %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(
            database_name, schema_name
        ) %}
        {% if target.type == "bigquery" %}
            {% set schema_exists_sql %}
                select count(*) as schema_count
                from `{{ database_name }}`.INFORMATION_SCHEMA.SCHEMATA
                where upper(schema_name) = upper('{{ tests_schema_name }}')
            {% endset %}
            {% set schema_exists_result = elementary.run_query(schema_exists_sql) %}
            {% set schema_exists = (
                schema_exists_result is not none
                and schema_exists_result.rows | length > 0
                and schema_exists_result.rows[0][0] | int > 0
            ) %}
        {% else %}
            {% set schema_exists = adapter.check_schema_exists(database_name, tests_schema_name) %}
        {% endif %}
        {%- if tests_schema_name != schema_name and not schema_exists %}
            {{ elementary.edr_log("Creating Elementary's tests schema.") }}
            {% set schema_relation = api.Relation.create(
                database=database_name, schema=tests_schema_name
            ).without_identifier() %}
            {%- do dbt.create_schema(schema_relation) %}
            {% do adapter.commit() %}
        {%- endif %}
    {% endif %}
    {{ return("") }}
{% endmacro %}
