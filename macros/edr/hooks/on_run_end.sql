{% macro on_run_end() %}
  {{ elementary.handle_tests_results(results) if flags.WHICH in ['test', 'build'] }}
  {{ elementary.upload_run_results(results) }}
{% endmacro %}
