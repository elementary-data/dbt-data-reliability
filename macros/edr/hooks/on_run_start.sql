{% macro on_run_start() %}
    {{ elementary.create_elementary_tests_schema() if flags.WHICH in ['test', 'build'] }}
{% endmacro %}
