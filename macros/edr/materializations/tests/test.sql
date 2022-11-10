{% macro query_test_result_rows(sample_limit=none) %}
  {%- set test_result_rows_sql -%}
      select * from ({{ sql }}) {%- if sample_limit -%} limit {{ sample_limit }} {%- endif -%}
  {%- endset -%}
  {% do return(dbt.run_query(test_result_rows_sql)) %}
{% endmacro %}

{% macro cache_test_result_rows(test_result_rows) %}
  {% do elementary.get_cache("test_results").update({model.unique_id: test_result_rows}) %}
{% endmacro %}


{% macro handle_anomaly_test(flattened_test) %}
  {% set metrics_tables_cache = elementary.get_cache("tables").get("metrics") %}
  {% set metrics_table = elementary.get_elementary_test_table(flattened_test.name, 'metrics') %}
  {% if metrics_table %}
    {% do metrics_tables_cache.append(metrics_table) %}
  {% endif %}

  {% set test_result_rows = [] %}
  {% for test_result_row in elementary.query_test_result_rows() %}
    {% do test_result_rows.append(elementary.get_anomaly_test_result_row(flattened_test, test_result_row)) %}
  {% endfor %}
  {% do elementary.cache_test_result_rows(test_result_rows) %}

  {% do context.update({"sql": elementary.get_anomaly_query(flattened_test)}) %}
{% endmacro %}

{% macro handle_schema_changes_test(flattened_test) %}
  {% set schema_snapshots_tables_cache = elementary.get_cache("tables").get("schema_snapshots") %}
  {% set schema_snapshots_table = elementary.get_elementary_test_table(flattened_test.name, 'schema_changes') %}
  {% if schema_snapshots_table %}
    {% do schema_snapshots_tables_cache.append(schema_snapshots_table) %}
  {% endif %}

  {% set test_result_rows = [] %}
  {% for test_result_row in elementary.query_test_result_rows() %}
    {% do test_result_rows.append(elementary.get_dbt_test_result_row(flattened_test, test_result_row, 'schema_change')) %}
  {% endfor %}
  {% do elementary.cache_test_result_rows(test_result_rows) %}
{% endmacro %}

{% macro handle_dbt_test(flattened_test) %}
  {% set test_result_rows = [] %}
  {% for test_result_row in elementary.query_test_result_rows(elementary.get_config_var('test_sample_row_count')) %}
    {% do test_result_rows.append(elementary.get_dbt_test_result_row(flattened_test, test_result_row, 'dbt_test')) %}
  {% endfor %}
  {% do elementary.cache_test_result_rows(test_result_rows) %}
{% endmacro %}

{% macro materialize_test() %}
  {% set flattened_test = elementary.flatten_test(model) %}
  {% if flattened_test.test_namespace == "elementary" %}
    {% if flattened_test.short_name.endswith("anomalies") %}
      {% do return(elementary.handle_anomaly_test(flattened_test)) %}
    {% elif flattened_test.short_name == 'schema_changes' %}
      {% do return(elementary.handle_schema_changes_test(flattened_test)) %}
    {% endif %}
  {% endif %}
  {% do return(elementary.handle_dbt_test(flattened_test)) %}
{% endmacro %}

{% materialization test, default %}
  {% do elementary.materialize_test() %}
  {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% do elementary.materialize_test() %}
  {{ return(dbt.materialization_test_snowflake()) }}
{% endmaterialization %}


{% macro get_anomaly_test_result_row(flattened_test, elementary_test_row) %}
  {% set full_table_name = elementary.insensitive_get_dict_value(elementary_test_row, 'full_table_name') %}
  {% set database_name, schema_name, table_name = elementary.split_full_table_name_to_vars(full_table_name) %}
  {% set test_params = elementary.insensitive_get_dict_value(flattened_test, 'test_params', {}) %}
  {% set sensitivity = elementary.insensitive_get_dict_value(test_params, 'sensitivity') or elementary.get_config_var('anomaly_sensitivity') %}
  {% set backfill_days = elementary.insensitive_get_dict_value(test_params, 'backfill_days') or elementary.get_config_var('backfill_days') %}
  {% set timestamp_column = elementary.insensitive_get_dict_value(test_params, 'timestamp_column') %}
  {% if not timestamp_column %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
    {% set parent_model_node = elementary.get_node(parent_model_unique_id) %}
    {% set timestamp_column = elementary.get_timestamp_column(timestamp_column, parent_model_node) %}
  {% endif %}
  {% do test_params.update({'sensitivity': sensitivity, 'timestamp_column': timestamp_column, 'backfill_days': backfill_days}) %}
  {% set column_name = elementary.insensitive_get_dict_value(elementary_test_row, 'column_name') %}
  {% set metric_name = elementary.insensitive_get_dict_value(elementary_test_row, 'metric_name') %}
  {% set metric_id = elementary.insensitive_get_dict_value(elementary_test_row, 'metric_id') %}
  {% set backfill_period = "'-" ~ backfill_days ~ "'" %}
  {% set test_results_query %}
      with anomaly_scores as (
          select * from {{ elementary.get_elementary_test_table(flattened_test.name, 'anomaly_scores') }}
      ),
      anomaly_scores_with_is_anomalous as (
      select  *,
              case when abs(anomaly_score) > {{ sensitivity }}
              and bucket_end >= {{ elementary.timeadd('day', backfill_period, elementary.get_max_bucket_end()) }}
              and training_set_size >= {{ elementary.get_config_var('days_back') -1 }} then TRUE else FALSE end as is_anomalous
          from anomaly_scores
          where anomaly_score is not null
      )
      select metric_value as value,
             training_avg as average,
             case when is_anomalous = TRUE then
              lag(min_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
             else min_metric_value end as min_value,
             case when is_anomalous = TRUE then
              lag(max_metric_value) over (partition by full_table_name, column_name, metric_name, dimension, dimension_value order by bucket_end)
              else max_metric_value end as max_value,
             bucket_start as start_time,
             bucket_end as end_time,
             metric_id
      from anomaly_scores_with_is_anomalous
      where upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }})
            and metric_name = {{ elementary.const_as_string(metric_name) }}
           {%- if column_name %}
              and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
           {%- endif %}
      order by bucket_end
  {%- endset -%}
  {% set test_results_description %}
      {% if elementary.insensitive_get_dict_value(elementary_test_row, 'anomaly_score') is none %}
          Not enough data to calculate anomaly score.
      {% else %}
          {{ elementary.insensitive_get_dict_value(elementary_test_row, 'anomaly_description') }}
      {% endif %}
  {% endset %}
  {% set test_result_dict = {
      'id': elementary.insensitive_get_dict_value(elementary_test_row, 'id'),
      'data_issue_id': elementary.insensitive_get_dict_value(elementary_test_row, 'metric_id'),
      'test_execution_id': elementary.insensitive_get_dict_value(elementary_test_row, 'test_execution_id'),
      'test_unique_id': elementary.insensitive_get_dict_value(elementary_test_row, 'test_unique_id'),
      'model_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id'),
      'detected_at': elementary.insensitive_get_dict_value(elementary_test_row, 'detected_at'),
      'database_name': database_name,
      'schema_name': schema_name,
      'table_name': table_name,
      'column_name': column_name,
      'test_type': 'anomaly_detection',
      'test_sub_type': metric_name,
      'test_results_description': test_results_description,
      'other': elementary.insensitive_get_dict_value(elementary_test_row, 'anomalous_value'),
      'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
      'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags'),
      'test_results_query': test_results_query,
      'test_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
      'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
      'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
      'result_rows': elementary.render_test_result_rows(elementary.get_cached_test_result_rows(flattened_test))
  } %}
  {{ return(test_result_dict) }}
{% endmacro %}

{% macro get_dbt_test_result_row(flattened_test, test_result_row, test_type) %}
    {% set test_execution_id = elementary.get_node_execution_id(flattened_test) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set test_short_name = elementary.insensitive_get_dict_value(flattened_test, 'short_name') %}
    {% set test_long_name = elementary.insensitive_get_dict_value(flattened_test, 'name') %}
    {%- if test_short_name -%}
        {% set test_name = test_short_name %}
    {%- else -%}
        {% set test_name = test_long_name %}
    {%- endif -%}
    {% set test_results_query = elementary.get_compiled_code(flattened_test) %}
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
        'test_type': test_type,
        'test_sub_type': elementary.insensitive_get_dict_value(flattened_test, 'type'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags'),
        'test_results_query': test_results_query,
        'test_name': test_name,
        'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
        'status': elementary.insensitive_get_dict_value(run_result_dict, 'status'),
        'failures': elementary.insensitive_get_dict_value(run_result_dict, 'failures'),
        'result_rows': elementary.render_test_result_rows(elementary.get_cached_test_result_rows(flattened_test))
    }%}
    {{ return(test_result_dict) }}
{% endmacro %}
