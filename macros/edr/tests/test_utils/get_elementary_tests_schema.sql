{% macro get_elementary_tests_schema(elementary_database, elementary_schema) %}
    {% set LEGACY_TESTS_SCHEMA_SUFFIX = "__tests" %}

    {% set cached_tests_schema_name = elementary.get_cache("tests_schema_name") %}
    {% if cached_tests_schema_name is not none %}
        {{ return(cached_tests_schema_name) }}
    {% endif %}

    {% set tests_schema_suffix = elementary.get_config_var('tests_schema_name') %}
    {% set tests_schema_name = elementary_schema ~ tests_schema_suffix %}

    {# Backward compatibility - if a tests schema suffix is not defined, but the legacy tests schema exists in the DB,
       then use it #}
    {% if not tests_schema_suffix %}
        {% set legacy_tests_schema_name = elementary_schema ~ LEGACY_TESTS_SCHEMA_SUFFIX %}
        {% if adapter.check_schema_exists(elementary_database, legacy_tests_schema_name) %}
            {% set tests_schema_name = legacy_tests_schema_name %}
        {% endif %}
    {% endif %}

    {% do elementary.set_cache("tests_schema_name", tests_schema_name) %}

    {{ return(tests_schema_name) }}
{% endmacro %}
