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
  {% set failed_count_config = elementary.get_common_test_failed_count_config(flattened_test) %}
  {% set test_meta = flattened_test['meta'] %}
  {% if 'elementary_failed_count' in test_meta %}
    {% do failed_count_config.update(test_meta['elementary_failed_count']) %}
  {% endif %}
  {% set query = elementary.get_configured_failed_test_result_count_query(flattened_test, failed_count_config['method'], failed_count_config.get('parameters', [])) %}
  {% do return(query) %}
{% endmacro %}

{% macro get_common_test_failed_count_config(flattened_test) %}
  {% set common_test_config = elementary.get_common_test_config_by_flattened_test(flattened_test) %}
  {% if common_test_config %}
    {% do return(common_test_config.get('failed_count', {})) %}
  {% endif %}
  {% do return({}) %}
{% endmacro %}

{% macro get_configured_failed_test_result_count_query(flattened_test, method, parameters) %}
  {% if method == 'sum' %}
    {% set sum_field = parameters[0] %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_sum(flattened_test, sum_field) %}
  {% elif method == 'expression' %}
    {% set expression = parameters[0] %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_expression(flattened_test, expression) %}
  {% elif method == 'count' %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_count(flattened_test) %}
  {% else %}
    {% set result_count_query = none %}
  {% endif %}
  {% do return(result_count_query) %}
{% endmacro %}

{% macro get_failed_test_result_count_query_by_count(flattened_test) %}
  {% set expression %}
    count(*)
  {% endset %}
  {% do return(elementary.get_failed_test_result_count_query_by_expression(flattened_test, expression)) %}
{% endmacro %}

{% macro get_failed_test_result_count_query_by_sum(flattened_test, field) %}
  {% set expression %}
    sum({{ field }})
  {% endset %}
  {% do return(elementary.get_failed_test_result_count_query_by_expression(flattened_test, expression)) %}
{% endmacro %}

{% macro get_failed_test_result_count_query_by_expression(flattened_test, expression) %}
  with results as (
    {{ flattened_test['compiled_code'] }}
  )
  select {{ expression }} as count from results
{% endmacro %}
