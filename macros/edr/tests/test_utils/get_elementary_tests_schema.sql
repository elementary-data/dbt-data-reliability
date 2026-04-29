{% macro get_elementary_tests_schema(elementary_database, elementary_schema) %}
    {% set LEGACY_TESTS_SCHEMA_SUFFIX = "__tests" %}

    {% set cached_tests_schema_name = elementary.get_cache("tests_schema_name") %}
    {% if cached_tests_schema_name is not none %}
        {{ return(cached_tests_schema_name) }}
    {% endif %}

    {% set tests_schema_suffix = elementary.get_config_var("tests_schema_name") %}
    {% set tests_schema_name = elementary_schema ~ tests_schema_suffix %}

    {# Backward compatibility - if a tests schema suffix is not defined, but the legacy tests schema exists in the DB,
       then use it #}
    {% if not tests_schema_suffix %}
        {% set legacy_tests_schema_name = (
            elementary_schema ~ LEGACY_TESTS_SCHEMA_SUFFIX
        ) %}
        {% if target.type == "bigquery" %}
            {% set legacy_schema_exists_sql %}
                select count(*) as schema_count
                from `{{ elementary_database }}`.INFORMATION_SCHEMA.SCHEMATA
                where upper(schema_name) = upper('{{ legacy_tests_schema_name }}')
            {% endset %}
            {% set legacy_schema_exists_result = elementary.run_query(legacy_schema_exists_sql) %}
            {% set legacy_schema_count_rows = [] %}
            {% if legacy_schema_exists_result is not none %}
                {% set legacy_schema_count_rows = elementary.agate_to_dicts(legacy_schema_exists_result) %}
            {% endif %}
            {% set legacy_schema_exists = (
                legacy_schema_count_rows | length > 0
                and legacy_schema_count_rows[0]["schema_count"] | int > 0
            ) %}
        {% else %}
            {% set legacy_schema_exists = adapter.check_schema_exists(
                elementary_database, legacy_tests_schema_name
            ) %}
        {% endif %}
        {% if legacy_schema_exists %}
            {% set tests_schema_name = legacy_tests_schema_name %}
        {% endif %}
    {% endif %}

    {% do elementary.set_cache("tests_schema_name", tests_schema_name) %}

    {{ return(tests_schema_name) }}
{% endmacro %}