{% macro get_test_result() %}
{#
  This macro should return a dictionary with the following keys:
  - failures - the result of dbts fail_calc
  - should_warn
  - should_error
#}
  {% set result = load_result('main') %}
  {% set rows = elementary.agate_to_dicts(result.table) %}
  {% do return(rows[0]) %}
{% endmacro %}


{% macro did_test_pass(test_result=none) %}
  {% if test_result is none %}
    {% set test_result = elementary.get_test_result() %}
  {% endif %}
  {% do return(not test_result.should_warn and not test_result.should_error) %}
{% endmacro %}
