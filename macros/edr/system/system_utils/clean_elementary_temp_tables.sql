{% macro clean_elementary_temp_tables() %}
    {% do elementary.clean_current_invocation_test_tables() %}
{% endmacro %}
