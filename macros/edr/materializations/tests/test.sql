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
    {% do elementary_test_results_rows.append(elementary.get_schema_changes_test_result_row(flattened_test, schema_changes_row, schema_changes_rows)) %}
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
    {% elif flattened_test.short_name.startswith('schema_changes') %}
      {% do return("schema_change") %}
    {% endif %}
  {% endif %}
  {% do return("dbt_test") %}
{% endmacro %}

{% macro materialize_test() %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(none) %}
  {% endif %}

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
  {%- if dbt.materialization_test_snowflake -%}
    {{ return(dbt.materialization_test_snowflake()) }}
  {%- else -%}
    {{ return(dbt.materialization_test_default()) }}
  {%- endif -%}
{% endmaterialization %}


{% macro get_anomaly_test_result_row(flattened_test, anomaly_scores_rows) %}
  {% set latest_row = anomaly_scores_rows[-1] %}
  {% set full_table_name = elementary.insensitive_get_dict_value(latest_row, 'full_table_name') %}
  {% set test_params = elementary.insensitive_get_dict_value(flattened_test, 'test_params') %}
  {% set sensitivity = elementary.insensitive_get_dict_value(test_params, 'sensitivity') or elementary.get_config_var('anomaly_sensitivity') %}
  {% set backfill_days = elementary.insensitive_get_dict_value(test_params, 'backfill_days') or elementary.get_config_var('backfill_days') %}
  {% set timestamp_column = elementary.insensitive_get_dict_value(test_params, 'timestamp_column') %}
  {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
  {% if not timestamp_column %}
    {% set parent_model_node = elementary.get_node(parent_model_unique_id) %}
    {% set timestamp_column = elementary.get_timestamp_column(timestamp_column, parent_model_node) %}
  {% endif %}
  {% do test_params.update({'sensitivity': sensitivity, 'timestamp_column': timestamp_column, 'backfill_days': backfill_days}) %}
  {% set column_name = elementary.insensitive_get_dict_value(latest_row, 'column_name') %}
  {% set metric_name = elementary.insensitive_get_dict_value(latest_row, 'metric_name') %}
  {% set backfill_days = elementary.insensitive_get_dict_value(test_params, 'backfill_days') %}
  {% set backfill_period = "'-" ~ backfill_days ~ "'" %}
  {% set test_unique_id = elementary.insensitive_get_dict_value(latest_row, 'test_unique_id') %}
  {% set has_anomaly_score = elementary.insensitive_get_dict_value(latest_row, 'anomaly_score') is not none %}
  {% if not has_anomaly_score %}
    {% do elementary.edr_log("Not enough data to calculate anomaly scores on `{}`".format(test_unique_id)) %}
  {% endif %}
  {%- set test_results_query -%}
      select * from ({{ sql }})
      where
        anomaly_score is not null and
        upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }}) and
        metric_name = {{ elementary.const_as_string(metric_name) }}
        {%- if column_name %}
          and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
        {%- endif %}
  {%- endset -%}
  {% set test_results_description %}
      {% if has_anomaly_score %}
          {{ elementary.insensitive_get_dict_value(latest_row, 'anomaly_description') }}
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
      'result_rows': elementary.render_result_rows(filtered_anomaly_scores_rows),
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
        'test_type': elementary.get_elementary_test_type(flattened_test),
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
        'result_rows': elementary.render_result_rows(result_rows)
    }%}
    {% do return(test_result_dict) %}
{% endmacro %}

{% macro render_result_rows(test_result_rows) %}
  {% if (tojson(test_result_rows) | length) < elementary.get_column_size() %}
    {% do return(test_result_rows) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}
