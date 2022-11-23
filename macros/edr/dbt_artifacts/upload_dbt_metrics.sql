{%- macro upload_dbt_metrics(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_metrics') %}
    {% if execute and relation %}
        {% set metrics = graph.metrics.values() | selectattr('resource_type', '==', 'metric') %}
        {% do elementary.upload_artifacts_to_table(relation, metrics, elementary.flatten_metric, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}



{% macro get_dbt_metrics_empty_table_query() %}
    {% set dbt_metrics_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                   ('name', 'string'),
                                                                   ('label', 'string'),
                                                                   ('model', 'string'),
                                                                   ('type', 'string'),
                                                                   ('sql', 'long_string'),
                                                                   ('timestamp', 'string'),
                                                                   ('filters', 'long_string'),
                                                                   ('time_grains', 'long_string'),
                                                                   ('dimensions', 'long_string'),
                                                                   ('depends_on_macros', 'long_string'),
                                                                   ('depends_on_nodes', 'long_string'),
                                                                   ('description', 'long_string'),
                                                                   ('tags', 'long_string'),
                                                                   ('meta', 'long_string'),
                                                                   ('package_name', 'string'),
                                                                   ('original_path', 'long_string'),
                                                                   ('path', 'string'),
                                                                   ('generated_at', 'string')]) %}
    {{ return(dbt_metrics_empty_table_query) }}
{% endmacro %}

{% macro flatten_metric(node_dict) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% set tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set flatten_metrics_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'name': node_dict.get('name'),
        'label': node_dict.get('label'),
        'model': node_dict.get('model'),
        'type': node_dict.get('type'),
        'sql': node_dict.get('sql'),
        'timestamp': node_dict.get('timestamp'),
        'filters': node_dict.get('filters', {}),
        'time_grains': node_dict.get('time_grains', []),
        'dimensions': node_dict.get('dimensions', []),
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'description': node_dict.get('description'),
        'tags': tags,
        'meta': meta_dict,
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string()
    }%}
    {{ return(flatten_metrics_metadata_dict) }}
{% endmacro %}
