{% macro upload_source_freshness_results() %}
  {% set source_freshness_results_relation = ref('source_freshness_results') %}
  {% set sources_json_path = flags.Path(ref.config.target_path ~ '/sources.json') %}
  {% if not sources_json_path.exists() %}
    {% do exceptions.raise_compiler_error('Source freshness artifact (sources.json) does not exist, please run `dbt source freshness`.') %}
  {% else %}
    {% set source_freshess_results_dicts = fromjson(sources_json_path.read_text())['results'] %}
  {% endif %}
  {% do elementary.upload_artifacts_to_table(source_freshness_results_relation, source_freshess_results_dicts, elementary.get_flatten_source_freshness_callback()) %}
{% endmacro %}

{%- macro get_flatten_source_freshness_callback() -%}
    {{- return(adapter.dispatch('flatten_source_freshness', 'elementary')) -}}
{%- endmacro -%}

{%- macro flatten_source_freshness(node_dict) -%}
    {{- return(adapter.dispatch('flatten_source_freshness', 'elementary')(node_dict)) -}}
{%- endmacro -%}

{% macro default__flatten_source_freshness(node_dict) %}
  {% set flatten_source_freshness_dict = {
       'source_freshness_execution_id': [invocation_id, node_dict.get('unique_id')] | join('.'),
       'unique_id': node_dict.get('unique_id'),
       'max_loaded_at': node_dict.get('max_loaded_at'),
       'snapshotted_at': node_dict.get('snapshotted_at'),
       'max_loaded_at_time_ago_in_s': node_dict.get('max_loaded_at_time_ago_in_s'),
       'status': node_dict.get('status'),
       'error': node_dict.get('error'),
       'generated_at': elementary.datetime_now_utc_as_string(),
       'invocation_id': invocation_id,
       'compile_started_at': node_dict.get('timing/0/started_at'),
       'compile_completed_at': node_dict.get('timing/0/completed_at'),
       'execute_started_at': node_dict.get('timing/1/started_at'),
       'execute_completed_at': node_dict.get('timing/1/completed_at')
   } %}
  {{ return(flatten_source_freshness_dict) }}
{% endmacro %}
