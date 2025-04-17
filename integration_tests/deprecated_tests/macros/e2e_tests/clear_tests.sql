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
    {% do adapter.dispatch('drop_schema')(database_name, schema_name) %}
{% endmacro %}

{% macro default__drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("dropped schema {}".format(schema_relation | string)) %}
{% endmacro %}

{% macro clickhouse__drop_schema(database_name, schema_name) %}
   {% set results = run_query("SELECT name FROM system.tables WHERE database = '" ~ database_name ~ "'") %}
    {% if execute %}
        {% for row in results %}
            {% set table = row[0] %}
            {% do run_query("DROP TABLE IF EXISTS " ~ database_name ~ "." ~ table) %}
        {% endfor %}
    {% endif %}
    {% do adapter.commit() %}
    {% do elementary.edr_log("dropped schema {}".format(schema_name)) %}
{% endmacro %}
