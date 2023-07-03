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
  {% set test_name = flattened_test['short_name'] %}
  {% set test_namespace = flattened_test['test_namespace'] %}
  {% if test_namespace is none %}
    {% set test_namespace = 'dbt' %}
  {% endif %}

  {% set test_meta = flattened_test['meta'] %}
  {% set failed_count_method = test_meta.get('elementary_failed_count_method') %}
  {% if failed_count_method %}
    {% set parameters = [] %}
    {% if failed_count_method == 'sum' %}
      {% set failed_count_sum_field = test_meta['elementary_failed_count_sum_field'] %}
      {% do parameters.append(failed_count_sum_field) %}
    {% endif %}
    {% do return(elementary.get_configured_failed_test_result_count_query(flattened_test, failed_count_method, parameters)) %}
  {% else %}
    {% do return(elementary.get_default_failed_test_result_count_query(flattened_test)) %}
  {% endif %}
{% endmacro %}

{% macro get_default_failed_test_result_count_query(flattened_test) %}
  {% set test_name_to_sum_field = {
    "dbt": {
      "unique": "n_records",
      "accepted_values": "n_records"
    }
  } %}
  {#test_table_anomalies, get_read_anomaly_scores_query, get_anomaly_query#}
  {% set count_test_names = {
    "dbt": [
      "not_null",
      "relationships"
    ],
    "elementary": [
      "json_schema"
    ]
   } %}
  {% set test_name = flattened_test['short_name'] %}
  {% set test_namespace = flattened_test['test_namespace'] %}
  {% if test_namespace is none %}
    {% set test_namespace = 'dbt' %}
  {% endif %}

  {% set method = '' %}
  {% set parameters = [] %}
  {% if test_namespace in test_name_to_sum_field and test_name in test_name_to_sum_field[test_namespace] %}
    {% set method = 'sum' %}
    {% set sum_field = test_name_to_sum_field[test_namespace][test_name] %}
    {% do parameters.append(sum_field) %}
  {% elif test_namespace in count_test_names and test_name in count_test_names[test_namespace] %}
    {% set method = 'count' %}
  {% endif %}
  {% do return(elementary.get_configured_failed_test_result_count_query(flattened_test, method, parameters)) %}
{% endmacro %}

{% macro get_configured_failed_test_result_count_query(flattened_test, method, parameters) %}
  {% if method == 'sum' %}
    {% set sum_field = parameters[0] %}
    {% set result_count_query = elementary.get_failed_test_result_count_query_by_sum(flattened_test, sum_field) %}
  {% elif method == 'count' %}
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
