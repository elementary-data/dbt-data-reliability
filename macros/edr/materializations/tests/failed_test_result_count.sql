{% macro get_failed_test_result_count(flattened_test) %}
  {% set result_count_query = elementary.get_failed_test_result_count_query(flattened_test) %}
  {% if result_count_query %}
    {% set result_count = elementary.result_value(result_count_query) %}
    {% do return(result_count) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_failed_test_result_count_query(flattened_test) %}
  {% set failed_count_calc = elementary.get_failed_count_calc(flattened_test) %}
  {% if failed_count_calc %}
    {% set result_count_query = elementary.get_failed_count_calc_query(failed_count_calc) %}
    {% do return(result_count_query) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_failed_count_calc(flattened_test) %}
  {% set common_test_config = elementary.get_common_test_config(flattened_test) %}
  {% if common_test_config %}
    {% if 'failed_count_calc' in common_test_config %}
      {% do return(common_test_config['failed_count_calc']) %}
    {% endif %}
  {% endif %}
  {% do return(flattened_test['meta'].get('failed_count_calc')) %}
{% endmacro %}

{% macro get_failed_count_calc_query(failed_count_calc) %}
  with results as (
    {{ sql }}
  )
  select {{ failed_count_calc }} as count from results
{% endmacro %}
