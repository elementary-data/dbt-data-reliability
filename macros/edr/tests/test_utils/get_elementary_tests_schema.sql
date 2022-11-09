{% macro get_elementary_tests_schema(elementary_database, elementary_schema) %}
    {% set LEGACY_TESTS_SCHEMA_SUFFIX = "__tests" %}

    {% set cached_tests_schema_name = elementary.get_cache_entry("tests_schema_name") %}
    {% if cached_tests_schema_name is not none %}
        {{ return(cached_tests_schema_name) }}
    {% endif %}

    {% set tests_schema_suffix = elementary.get_config_var('tests_schema_name') %}

    {% if not tests_schema_suffix %}
        {% if adapter.check_schema_exists(elementary_database, elementary_schema ~ LEGACY_TESTS_SCHEMA_SUFFIX) %}
            {% set tests_schema_suffix = LEGACY_TESTS_SCHEMA_SUFFIX %}
        {% endif %}
    {% endif %}

    {% set tests_schema_name = elementary_schema ~ tests_schema_suffix %}
    {% do elementary.set_cache_entry("tests_schema_name", tests_schema_name) %}

    {{ return(tests_schema_name) }}
{% endmacro %}
