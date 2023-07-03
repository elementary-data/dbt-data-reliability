{% macro handle_failed_test_result_count(flattened_test) %}
  {% set result_count_query = elementary.get_failed_test_result_count_query(flattened_test) %}
  {% if result_count_query %}
    {% set result_count = run_query(result_count_query).columns[0].values()[0] %}  
  {% else %}
    {% set result_count = none %}
  {% endif %}
  {% do elementary.get_cache('elementary_test_failed_count').update({model.unique_id: result_count}) %}
{% endmacro %}

{% macro get_failed_test_result_count_query(flattened_test) %}
  {% set test_name_to_sum_field = {
    "unique": "n_records",
    "accepted_values": "n_records"
  } %}
  {% set count_test_names = [
    "not_null",
    "relationships"
  ] %}
  {% set test_name = flattened_test['short_name'] %}
  {% if test_name in test_name_to_sum_field %}
    {% set sum_field = test_name_to_sum_field[test_name] %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_sum(flattened_test, sum_field) %}
  {% elif test_name in count_test_names %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_count(flattened_test) %}
  {% else %}
    {% set result_count_query = none %}
  {% endif %}
  {% do return(result_count_query) %}
{% endmacro %}

{% macro get_failed_test_result_count_query_by_count(flattened_test) %}
  with results as (
    {{ flattened_test['compiled_code'] }}
  )
  select count(*) AS count from results
{% endmacro %}

{% macro get_failed_test_result_count_query_by_sum(flattened_test, field) %}
  with results as (
    {{ flattened_test['compiled_code'] }}
  )
  select sum({{ field }}) as count from results
{% endmacro %}
