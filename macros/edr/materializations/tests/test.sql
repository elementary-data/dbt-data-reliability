{% macro query_test_result_rows(sample_limit=none) %}
  {%- set query -%}
      select * from ({{ sql }}) {%- if sample_limit -%} limit {{ sample_limit }} {%- endif -%}
  {%- endset -%}
  {% do return(elementary.agate_to_dicts(dbt.run_query(query))) %}
{% endmacro %}

{% macro cache_elementary_test_results_rows(elementary_test_results_rows) %}
  {% do elementary.get_cache("elementary_test_results").update({model.unique_id: elementary_test_results_rows}) %}
{% endmacro %}


{% macro handle_anomaly_test(flattened_test) %}
  {% set metrics_tables_cache = elementary.get_cache("tables").get("metrics") %}
  {% set metrics_table = elementary.get_elementary_test_table(flattened_test.name, 'metrics') %}
  {% if metrics_table %}
    {% do metrics_tables_cache.append(metrics_table) %}
  {% endif %}

  {% set anomaly_scores_rows = elementary.query_test_result_rows() %}
  {% if anomaly_scores_rows and flattened_test.short_name == "dimension_anomalies" %}
    {% set sample_row = anomaly_scores_rows[0] %}
    {% do elementary.cache_elementary_test_results_rows([elementary.get_anomaly_test_result_row(flattened_test, sample_row, anomaly_scores_rows)]) %}
  {% else %}
    {% set anomaly_scores_metrics_rows = {} %}
    {% for anomaly_scores_row in anomaly_scores_rows %}
      {% set anomaly_score_id = (anomaly_scores_row.full_table_name, anomaly_scores_row.column_name, anomaly_scores_row.metric_name) %}
      {% do anomaly_scores_metrics_rows.setdefault(anomaly_score_id, []) %}
      {% do anomaly_scores_metrics_rows[anomaly_score_id].append(anomaly_scores_row) %}
    {% endfor %}

    {% set elementary_test_results_rows = [] %}
    {% for metric_id, metric_rows in anomaly_scores_metrics_rows.items() %}
      {% do elementary.debug_log("Found {} anomaly scores for metric {}.".format(metric_rows | length, metric_id)) %}
      {% set sample_row = metric_rows[0] %}
      {% do elementary_test_results_rows.append(elementary.get_anomaly_test_result_row(flattened_test, sample_row, metric_rows)) %}
    {% endfor %}
    {% do elementary.cache_elementary_test_results_rows(elementary_test_results_rows) %}
  {% endif %}

  {% do context.update({"sql": elementary.get_anomaly_query(flattened_test)}) %}
{% endmacro %}

{% macro handle_schema_changes_test(flattened_test) %}
  {% set schema_snapshots_tables_cache = elementary.get_cache("tables").get("schema_snapshots") %}
  {% set schema_snapshots_table = elementary.get_elementary_test_table(flattened_test.name, 'schema_changes') %}
  {% if schema_snapshots_table %}
    {% do schema_snapshots_tables_cache.append(schema_snapshots_table) %}
  {% endif %}

  {% set elementary_test_results_rows = [] %}
  {% set schema_changes_rows = elementary.query_test_result_rows() %}
  {% for schema_changes_row in schema_changes_rows %}
    {% do elementary_test_results_rows.append(elementary.get_schema_changes_test_result_row(flattened_test, schema_changes_row)) %}
  {% endfor %}
  {% do elementary.cache_elementary_test_results_rows(elementary_test_results_rows) %}
{% endmacro %}

{% macro handle_dbt_test(flattened_test) %}
  {% set result_rows = elementary.query_test_result_rows(sample_limit=elementary.get_config_var('test_sample_row_count')) %}
  {% set elementary_test_results_row = elementary.get_dbt_test_result_row(flattened_test, result_rows) %}
  {% do elementary.cache_elementary_test_results_rows([elementary_test_results_row]) %}
{% endmacro %}

{% macro get_elementary_test_type(flattened_test) %}
  {% if flattened_test.test_namespace == "elementary" %}
    {% if flattened_test.short_name.endswith("anomalies") %}
      {% do return("anomaly_detection") %}
    {% elif flattened_test.short_name == 'schema_changes' %}
      {% do return("schema_change") %}
    {% endif %}
  {% endif %}
  {% do return("dbt_test") %}
{% endmacro %}

{% macro materialize_test() %}
  {% set flattened_test = elementary.flatten_test(model) %}
  {% set test_type = elementary.get_elementary_test_type(flattened_test) %}
  {% set test_type_handler_map = {
    "anomaly_detection": elementary.handle_anomaly_test,
    "schema_change": elementary.handle_schema_changes_test,
    "dbt_test": elementary.handle_dbt_test
  } %}
  {% set test_type_handler = test_type_handler_map.get(test_type) %}
  {% if not test_type_handler %}
    {% do exceptions.raise_compiler_error("Unknown test type: {}".format(test_type)) %}
  {% endif %}
  {% do test_type_handler(flattened_test) %}
{% endmacro %}

{% materialization test, default %}
  {% do elementary.materialize_test() %}
  {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% do elementary.materialize_test() %}
  {{ return(dbt.materialization_test_snowflake()) }}
{% endmaterialization %}


{% macro get_anomaly_test_result_row(flattened_test, anomaly_scores_row, anomaly_scores_rows) %}
  {% set full_table_name = elementary.insensitive_get_dict_value(anomaly_scores_row, 'full_table_name') %}
  {% set database_name, schema_name, table_name = elementary.split_full_table_name_to_vars(full_table_name) %}
  {% set test_params = elementary.insensitive_get_dict_value(flattened_test, 'test_params', {}) %}
  {% set sensitivity = elementary.insensitive_get_dict_value(test_params, 'sensitivity') or elementary.get_config_var('anomaly_sensitivity') %}
  {% set backfill_days = elementary.insensitive_get_dict_value(test_params, 'backfill_days') or elementary.get_config_var('backfill_days') %}
  {% set timestamp_column = elementary.insensitive_get_dict_value(test_params, 'timestamp_column') %}
  {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
  {% if not timestamp_column %}
    {% set parent_model_node = elementary.get_node(parent_model_unique_id) %}
    {% set timestamp_column = elementary.get_timestamp_column(timestamp_column, parent_model_node) %}
  {% endif %}
  {% do test_params.update({'sensitivity': sensitivity, 'timestamp_column': timestamp_column, 'backfill_days': backfill_days}) %}
  {% set column_name = elementary.insensitive_get_dict_value(anomaly_scores_row, 'column_name') %}
  {% set metric_name = elementary.insensitive_get_dict_value(anomaly_scores_row, 'metric_name') %}
  {% set metric_id = elementary.insensitive_get_dict_value(anomaly_scores_row, 'metric_id') %}
  {% set backfill_period = "'-" ~ backfill_days ~ "'" %}
  {% set test_unique_id = elementary.insensitive_get_dict_value(anomaly_scores_row, 'test_unique_id') %}
  {% set has_anomaly_score = elementary.insensitive_get_dict_value(anomaly_scores_row, 'anomaly_score') is not none %}
  {% if not has_anomaly_score %}
    {% do elementary.edr_log("Not enough data to calculate anomaly scores on `{}`".format(test_unique_id)) %}
  {% endif %}
  {%- set test_results_query -%}
      select * from ({{ elementary.get_read_anomaly_scores_query(flattened_test) }})
      where
        upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }}) and
        metric_name = {{ elementary.const_as_string(metric_name) }}
        {%- if column_name %}
          and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
        {%- endif %}
  {%- endset -%}
  {% set test_results_description %}
      {% if has_anomaly_score %}
          {{ elementary.insensitive_get_dict_value(anomaly_scores_row, 'anomaly_description') }}
      {% else %}
          Not enough data to calculate anomaly score.
      {% endif %}
  {% endset %}
  {% set test_result_dict = {
      'id': elementary.insensitive_get_dict_value(anomaly_scores_row, 'id'),
      'data_issue_id': elementary.insensitive_get_dict_value(anomaly_scores_row, 'metric_id'),
      'test_execution_id': elementary.insensitive_get_dict_value(anomaly_scores_row, 'test_execution_id'),
      'test_unique_id': test_unique_id,
      'model_unique_id': parent_model_unique_id,
      'detected_at': elementary.insensitive_get_dict_value(anomaly_scores_row, 'detected_at'),
      'database_name': database_name,
      'schema_name': schema_name,
      'table_name': table_name,
      'column_name': column_name,
      'test_type': 'anomaly_detection',
      'test_sub_type': metric_name,
      'test_results_description': test_results_description,
      'other': elementary.insensitive_get_dict_value(anomaly_scores_row, 'anomalous_value'),
      'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
      'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags'),
      'test_results_query': test_results_query,
      'test_name': elementary.insensitive_get_dict_value(flattened_test, 'name'),
      'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
      'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
      'test_short_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
      'test_alias': elementary.insensitive_get_dict_value(flattened_test, 'alias'),
      'result_rows': elementary.render_result_rows(anomaly_scores_rows)
  } %}
  {{ return(test_result_dict) }}
{% endmacro %}

{% macro get_schema_changes_test_result_row(flattened_test, elementary_test_row) %}
  {% do elementary_test_row.update({
    'other': none,
    'model_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id'),
    'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
    'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags'),
    'test_results_query': test_results_query,
    'test_name': elementary.insensitive_get_dict_value(flattened_test, 'name'),
    'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
    'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
    'test_short_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
    'test_alias': elementary.insensitive_get_dict_value(flattened_test, 'alias'),
    'test_type': "schema_change"
  }) %}
  {{ return(elementary_test_row) }}
{% endmacro %}

{% macro get_dbt_test_result_row(flattened_test, result_rows=none) %}
    {% set test_execution_id = elementary.get_node_execution_id(flattened_test) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set test_result_dict = {
        'id': test_execution_id,
        'data_issue_id': none,
        'test_execution_id': test_execution_id,
        'test_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'unique_id'),
        'model_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id'),
        'detected_at': elementary.insensitive_get_dict_value(flattened_test, 'generated_at'),
        'database_name': elementary.insensitive_get_dict_value(flattened_test, 'database_name'),
        'schema_name': elementary.insensitive_get_dict_value(flattened_test, 'schema_name'),
        'table_name': parent_model_name,
        'column_name': elementary.insensitive_get_dict_value(flattened_test, 'test_column_name'),
        'test_type': elementary.get_elementary_test_type(flattened_test),
        'test_sub_type': elementary.insensitive_get_dict_value(flattened_test, 'type'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags'),
        'test_results_query': elementary.get_compiled_code(flattened_test),
        'test_name': elementary.insensitive_get_dict_value(flattened_test, 'name'),
        'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
        'test_short_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
        'test_alias': elementary.insensitive_get_dict_value(flattened_test, 'alias'),
        'result_rows': elementary.render_result_rows(result_rows)
    }%}
    {{ return(test_result_dict) }}
{% endmacro %}

{% macro render_result_rows(test_result_rows) %}
  {% if (tojson(test_result_rows) | length) < elementary.get_column_size() %}
    {{ return(test_result_rows) }}
  {% endif %}
  {{ return(none) }}
{% endmacro %}
