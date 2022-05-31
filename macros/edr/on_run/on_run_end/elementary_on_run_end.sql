{% macro elementary_on_run_end() %}
    {{ elementary.handle_test_results(results) if flags.WHICH in ['test', 'build'] }}
    {{ elementary.update_invocation_end() }}
{% endmacro %}


