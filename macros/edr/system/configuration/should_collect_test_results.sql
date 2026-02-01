{% macro should_collect_test_results() %}
  {#
    Determine if Elementary should collect and cache test results.
    We skip collection if:
    1. We are NOT in the execution phase (compilation/parsing).
    2. Elementary infrastructure is not configured.
    3. Test results collection is explicitly disabled via config.
    4. This is an EDR CLI run (initialization is skipped in on_run_start).
  #}
  {% set is_enabled = elementary.is_elementary_enabled() %}
  {% set tests_results_enabled = not elementary.get_config_var('disable_tests_results') %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}

  {% set should_collect = execute and is_enabled and tests_results_enabled and not edr_cli_run %}
  {% do return(should_collect) %}
{% endmacro %}
