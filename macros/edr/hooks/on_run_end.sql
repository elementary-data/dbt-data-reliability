{% macro on_run_end() %}
  {{ elementary.upload_run_results(results) }}
  {{ handle_test_results(results) if flags.WHICH in ['test', 'build'] }}
{% endmacro %}
