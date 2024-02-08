{% macro clean_elementary_temp_tables() %}
    {% do elementary.clean_elementary_test_tables() %}
{% endmacro %}
