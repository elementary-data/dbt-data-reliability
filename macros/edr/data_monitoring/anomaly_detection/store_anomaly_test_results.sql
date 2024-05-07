{% macro store_anomaly_test_results(flattened_test, anomaly_scores_sql) %}
  {% set anomaly_scores_groups_rows = {} %}
  {% set anomaly_scores_rows = elementary.agate_to_dicts(elementary.run_query(anomaly_scores_sql)) %}
  {% for anomaly_scores_row in anomaly_scores_rows %}
    {% set anomaly_scores_group = (anomaly_scores_row.full_table_name, anomaly_scores_row.column_name, anomaly_scores_row.metric_name) %}
    {% do anomaly_scores_groups_rows.setdefault(anomaly_scores_group, []) %}
    {% do anomaly_scores_groups_rows[anomaly_scores_group].append(anomaly_scores_row) %}
  {% endfor %}

  {% set elementary_test_results_rows = [] %}
  {% for anomaly_scores_group, anomaly_scores_rows in anomaly_scores_groups_rows.items() %}
    {% do elementary.debug_log("Found {} anomaly scores for group {}.".format(anomaly_scores_rows | length, anomaly_scores_group)) %}
    {% do elementary_test_results_rows.append(elementary.get_anomaly_test_result_row(flattened_test, anomaly_scores_rows)) %}
  {% endfor %}
  {% do elementary.cache_elementary_test_results_rows(elementary_test_results_rows) %}
{% endmacro %}

{% macro get_anomaly_test_result_row(flattened_test, anomaly_scores_rows) %}
  {%- set latest_row = anomaly_scores_rows[-1] %}
  {%- set rows_with_score = anomaly_scores_rows | rejectattr("anomaly_score", "none") | list %}
  {%- set full_table_name = elementary.insensitive_get_dict_value(latest_row, 'full_table_name') %}
  {%- set test_unique_id = flattened_test.unique_id %}
  {%- set test_configuration = elementary.get_cache(test_unique_id) %}
  {%- set test_params = elementary.insensitive_get_dict_value(flattened_test, 'test_params') %}
  {%- do test_params.update(test_configuration) %}
  {%- set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
  {%- set column_name = elementary.insensitive_get_dict_value(latest_row, 'column_name') %}
  {%- set metric_name = elementary.insensitive_get_dict_value(latest_row, 'metric_name') %}
  {%- set test_unique_id = elementary.insensitive_get_dict_value(latest_row, 'test_unique_id') %}
  {%- if not rows_with_score %}
    {% do elementary.edr_log("Not enough data to calculate anomaly scores on `{}`".format(test_unique_id)) %}
  {% endif %}
  {%- set test_results_query -%}
      select * from ({{ sql }}) results
      where
        anomaly_score is not null and
        upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }}) and
        metric_name = {{ elementary.const_as_string(metric_name) }}
        {%- if column_name %}
          and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
        {%- endif %}
  {%- endset -%}
  {% set test_results_description %}
      {% if rows_with_score %}
          {{ elementary.insensitive_get_dict_value(rows_with_score[-1], 'anomaly_description') }}
      {% else %}
          Not enough data to calculate anomaly score.
      {% endif %}
  {% endset %}
  {% set failures = namespace(data=0) %}
  {% set filtered_anomaly_scores_rows = [] %}
  {% for row in anomaly_scores_rows %}
    {% if row.anomaly_score is not none %}
      {% do filtered_anomaly_scores_rows.append(row) %}
      {% if row.is_anomalous %}
        {% set failures.data = failures.data + 1 %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% set test_result_dict = {
      'id': elementary.insensitive_get_dict_value(latest_row, 'id'),
      'data_issue_id': elementary.insensitive_get_dict_value(latest_row, 'metric_id'),
      'model_unique_id': parent_model_unique_id,
      'column_name': column_name,
      'test_type': 'anomaly_detection',
      'test_sub_type': metric_name,
      'test_results_description': test_results_description,
      'other': elementary.insensitive_get_dict_value(latest_row, 'anomalous_value'),
      'test_results_query': test_results_query,
      'test_params': test_params,
      'result_rows': filtered_anomaly_scores_rows,
      'failures': failures.data
  } %}
  {% set elementary_test_row = elementary.get_dbt_test_result_row(flattened_test) %}
  {% do elementary_test_row.update(test_result_dict) %}
  {% do return(elementary_test_row) %}
{% endmacro %}
