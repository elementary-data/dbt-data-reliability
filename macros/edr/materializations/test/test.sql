{% macro create_test_result_temp_table() %}
  {% set database, schema = elementary.get_package_database_and_schema() %}
  {% set test_id = model["alias"] %}
  {% set relation = elementary.create_temp_table(database, schema, test_id, sql) %}
  {% set new_sql %}
    select * from {{ relation }}
  {% endset %}
  {% do return(new_sql) %}
{% endmacro %}

{% macro query_test_result_rows(sample_limit=none, ignore_passed_tests=false) %}
  {% if sample_limit == 0 %} {# performance: no need to run a sql query that we know returns an empty list #}
    {% do return([]) %}
  {% endif %}
  {% if ignore_passed_tests and elementary.did_test_pass() %}
    {% do elementary.debug_log("Skipping sample query because the test passed.") %}
    {% do return([]) %}
  {% endif %}
  {% set query %}
    with test_results as (
      {{ sql }}
    )
    select * from test_results {% if sample_limit is not none %} limit {{ sample_limit }} {% endif %}
  {% endset %}
  {% do return(elementary.agate_to_dicts(elementary.run_query(query))) %}
{% endmacro %}

{% macro cache_elementary_test_results_rows(elementary_test_results_rows) %}
  {% do elementary.get_cache("elementary_test_results").update({model.unique_id: elementary_test_results_rows}) %}
{% endmacro %}


{% macro handle_anomaly_test(flattened_test, materialization_macro) %}
  {% set metrics_tables_cache = elementary.get_cache("tables").get("metrics").get("relations") %}
  {% set metrics_table = elementary.get_elementary_test_table(elementary.get_elementary_test_table_name(), 'metrics') %}
  {% if metrics_table %}
    {% do metrics_tables_cache.append(metrics_table) %}
  {% endif %}

  {% set anomaly_scores_groups_rows = {} %}
  {% set anomaly_scores_rows = elementary.query_test_result_rows() %}
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

  {% do context.update({"sql": elementary.get_anomaly_query(flattened_test)}) %}
  {% do return(materialization_macro()) %}
{% endmacro %}

{% macro handle_schema_changes_test(flattened_test, materialization_macro) %}
  {% set schema_snapshots_tables_cache = elementary.get_cache("tables").get("schema_snapshots") %}
  {% set schema_snapshots_table = elementary.get_elementary_test_table(elementary.get_elementary_test_table_name(), 'schema_changes') %}
  {% if schema_snapshots_table %}
    {% do schema_snapshots_tables_cache.append(schema_snapshots_table) %}
  {% endif %}

  {% set elementary_test_results_rows = [] %}
  {% set schema_changes_rows = elementary.query_test_result_rows() %}
  {% for schema_changes_row in schema_changes_rows %}
    {% do elementary_test_results_rows.append(elementary.get_schema_changes_test_result_row(flattened_test, schema_changes_row, schema_changes_rows)) %}
  {% endfor %}
  {% do elementary.cache_elementary_test_results_rows(elementary_test_results_rows) %}
  {% do return(materialization_macro()) %}
{% endmacro %}

{% macro handle_dbt_test(flattened_test, materialization_macro) %}
  {% set result = materialization_macro() %}
  {% set result_rows = elementary.query_test_result_rows(sample_limit=elementary.get_config_var('test_sample_row_count'),
                                                         ignore_passed_tests=true) %}
  {% set elementary_test_results_row = elementary.get_dbt_test_result_row(flattened_test, result_rows) %}
  {% do elementary.cache_elementary_test_results_rows([elementary_test_results_row]) %}
  {% do return(result) %}
{% endmacro %}

{% macro get_test_type_handler(flattened_test) %}
  {% set test_type = elementary.get_test_type(flattened_test) %}
  {% set test_type_handler_map = {
    "anomaly_detection": elementary.handle_anomaly_test,
    "schema_change": elementary.handle_schema_changes_test,
    "dbt_test": elementary.handle_dbt_test,
    "integrity": elementary.handle_dbt_test
  } %}
  {% set test_type_handler = test_type_handler_map.get(test_type) %}
  {% if not test_type_handler %}
    {% do exceptions.raise_compiler_error("Unknown test type: {}".format(test_type)) %}
  {% endif %}
  {% do return(test_type_handler) %}
{% endmacro %}


{% macro materialize_test(materialization_macro) %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(materialization_macro()) %}
  {% endif %}

  {% set test_unique_id = model.get('unique_id') %}
  {% do elementary.debug_log(test_unique_id ~ ": starting test materialization hook") %}
  {% if elementary.get_config_var("tests_use_temp_tables") %}
    {% set temp_table_sql = elementary.create_test_result_temp_table() %}
    {% do context.update({"sql": temp_table_sql}) %}
    {% do elementary.debug_log(test_unique_id ~ ": created test temp table") %}
  {% endif %}

  {% set flattened_test = elementary.flatten_test(model) %}
  {% do elementary.debug_log(test_unique_id ~ ": flattened test node") %}
  {% set test_type_handler = elementary.get_test_type_handler(flattened_test) %}
  {% set result = test_type_handler(flattened_test, materialization_macro) %}
  {% do elementary.debug_log(test_unique_id ~ ": handler called by test type - " ~ elementary.get_test_type(flattened_test)) %}
  {% if elementary.get_config_var("calculate_failed_count") %}
    {% set failed_row_count = elementary.get_failed_row_count(flattened_test) %}
    {% if failed_row_count is not none %}
      {% do elementary.get_cache("elementary_test_failed_row_counts").update({model.unique_id: failed_row_count}) %}
      {% do elementary.debug_log(test_unique_id ~ ": calculated failed row count") %}
    {% endif %}
  {% endif %}
  {% do elementary.debug_log(test_unique_id ~ ": finished test materialization hook") %}
  {% do return(result) %}
{% endmacro %}

{% materialization test, default %}
  {% set result = elementary.materialize_test(dbt.materialization_test_default) %}
  {% do return(result) %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {%- if dbt.materialization_test_snowflake -%}
    {% set materialization_macro = dbt.materialization_test_snowflake %}
  {%- else -%}
    {% set materialization_macro = dbt.materialization_test_default %}
  {%- endif -%}
  {% set result = elementary.materialize_test(materialization_macro) %}
  {% do return(result) %}
{% endmaterialization %}


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

{% macro get_schema_changes_test_result_row(flattened_test, schema_changes_row, schema_changes_rows) %}
  {% set elementary_test_row = elementary.get_dbt_test_result_row(flattened_test, schema_changes_rows) %}
  {% do elementary_test_row.update(schema_changes_row) %}
  {% do return(elementary_test_row) %}
{% endmacro %}

{% macro get_dbt_test_result_row(flattened_test, result_rows=none) %}
    {% if not result_rows %}
      {% set result_rows = [] %}
    {% endif %}

    {% set test_execution_id = elementary.get_node_execution_id(flattened_test) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set test_result_dict = {
        'id': test_execution_id,
        'data_issue_id': none,
        'test_execution_id': test_execution_id,
        'test_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'unique_id'),
        'model_unique_id': parent_model_unique_id,
        'detected_at': elementary.insensitive_get_dict_value(flattened_test, 'generated_at'),
        'database_name': elementary.insensitive_get_dict_value(flattened_test, 'database_name'),
        'schema_name': elementary.insensitive_get_dict_value(flattened_test, 'schema_name'),
        'table_name': parent_model_name,
        'column_name': elementary.insensitive_get_dict_value(flattened_test, 'test_column_name'),
        'test_type': elementary.get_test_type(flattened_test),
        'test_sub_type': elementary.insensitive_get_dict_value(flattened_test, 'type'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags') + elementary.insensitive_get_dict_value(flattened_test, 'tags'),
        'test_results_query': elementary.get_compiled_code(flattened_test),
        'test_name': elementary.insensitive_get_dict_value(flattened_test, 'name'),
        'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
        'test_short_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
        'test_alias': elementary.insensitive_get_dict_value(flattened_test, 'alias'),
        'result_rows': result_rows
    }%}
    {% do return(test_result_dict) %}
{% endmacro %}
