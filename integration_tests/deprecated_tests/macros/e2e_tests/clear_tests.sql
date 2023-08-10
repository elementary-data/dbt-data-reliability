{% macro clear_tests() %}
    {% if execute %}
        {% do drop_schema(elementary.target_database(), target.schema) %}

        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% do drop_schema(database_name, schema_name) %}

        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}
        {% if tests_schema_name != schema_name %}
            {% do drop_schema(database_name, tests_schema_name) %}
        {% else %}
            {{ elementary.edr_log("Tests schema is the same as the main elementary schema, nothing to drop.") }}
        {% endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}

{% macro drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("dropped schema {}".format(schema_relation | string)) %}
{% endmacro %}
