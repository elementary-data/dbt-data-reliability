{% macro upload_source_freshness_results() %}
  {% set source_freshness_results_relation = ref('elementary', 'source_freshness_results') %}
  {% set sources_json_path = ref.config.target_path ~ '/sources.json' %}
  {% set empty_agate = run_query('SELECT 1') %}
  {% set source_freshess_results_agate = empty_agate.from_json(sources_json_path, key='results') %}
  {% set source_freshess_results_dicts = elementary.agate_to_dicts(source_freshess_results_agate) %}
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
         'status': node_dict.get('status')
     }%}
    {{ return(flatten_source_freshness_dict) }}
{% endmacro %}
