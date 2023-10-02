{% macro upload_source_freshness() %}
  {% if not flags.Path %}
    {% do exceptions.raise_compiler_error("Unable to upload source freshness using this operation on dbt 1.3.2 onwards.") %}
  {% endif %}

  {% set source_freshness_results_relation = ref('dbt_source_freshness_results') %}
  {% set sources_json_path = flags.Path(elementary.get_runtime_config().target_path).joinpath('sources.json') %}
  {% if not sources_json_path.exists() %}
    {% do exceptions.raise_compiler_error('Source freshness artifact (sources.json) does not exist, please run `dbt source freshness`.') %}
  {% endif %}
  {% set source_freshness_results_dicts = fromjson(sources_json_path.read_text())['results'] %}
  {% do elementary.upload_artifacts_to_table(source_freshness_results_relation, source_freshness_results_dicts, elementary.flatten_source_freshness, append=True, should_commit=True) %}
{% endmacro %}

{% macro flatten_source_freshness(node_dict) %}
  {% set compile_timing = {} %}
  {% set execute_timing = {} %}
  {% for timing in node_dict['timing'] %}
    {% if timing['name'] == 'compile' %}
      {% do compile_timing.update(timing) %}
    {% elif timing['name'] == 'execute' %}
      {% do execute_timing.update(timing) %}
    {% endif %}
  {% endfor %}
  {% set metadata_dict = elementary.safe_get_with_default(node_dict, 'metadata', {}) %}
  {% set criteria_dict = elementary.safe_get_with_default(node_dict, 'criteria', {}) %}
  {% set source_freshness_invocation_id = metadata_dict.get('invocation_id', invocation_id) %}
  {% set flatten_source_freshness_dict = {
       'source_freshness_execution_id': [source_freshness_invocation_id, node_dict.get('unique_id')] | join('.'),
       'unique_id': node_dict.get('unique_id'),
       'max_loaded_at': node_dict.get('max_loaded_at'),
       'snapshotted_at': node_dict.get('snapshotted_at'),
       'max_loaded_at_time_ago_in_s': node_dict.get('max_loaded_at_time_ago_in_s'),
       'status': node_dict.get('status'),
       'error': node_dict.get('error'),
       'warn_after': criteria_dict.get('warn_after'),
       'error_after': criteria_dict.get('error_after'),
       'filter': criteria_dict.get('filter'),
       'generated_at': elementary.datetime_now_utc_as_string(),
       'invocation_id': source_freshness_invocation_id,
       'compile_started_at': compile_timing.get('started_at'),
       'compile_completed_at': compile_timing.get('completed_at'),
       'execute_started_at': execute_timing.get('started_at'),
       'execute_completed_at': execute_timing.get('completed_at'),
   } %}
  {{ return(flatten_source_freshness_dict) }}
{% endmacro %}
