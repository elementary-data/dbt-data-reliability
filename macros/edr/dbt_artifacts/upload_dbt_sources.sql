{%- macro upload_dbt_sources(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_sources') %}
    {% if execute and relation %}
        {% set sources = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {% do elementary.upload_artifacts_to_table(relation, sources, elementary.flatten_source, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}



{% macro get_dbt_sources_empty_table_query() %}
    {% set dbt_sources_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                   ('database_name', 'string'),
                                                                   ('schema_name', 'string'),
                                                                   ('source_name', 'string'),
                                                                   ('name', 'string'),
                                                                   ('identifier', 'string'),
                                                                   ('loaded_at_field', 'string'),
                                                                   ('freshness_warn_after', 'string'),
                                                                   ('freshness_error_after', 'string'),
                                                                   ('freshness_filter', 'long_string'),
                                                                   ('relation_name', 'string'),
                                                                   ('tags', 'long_string'),
                                                                   ('meta', 'long_string'),
                                                                   ('owner', 'string'),
                                                                   ('package_name', 'string'),
                                                                   ('original_path', 'long_string'),
                                                                   ('path', 'string'),
                                                                   ('source_description', 'long_string'),
                                                                   ('description', 'long_string'),
                                                                   ('generated_at', 'string')]) %}
    {{ return(dbt_sources_empty_table_query) }}
{% endmacro %}

{% macro flatten_source(node_dict) %}
    {% set freshness_dict = elementary.safe_get_with_default(node_dict, 'freshness', {}) %}
    {% set source_meta_dict = elementary.safe_get_with_default(node_dict, 'source_meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(source_meta_dict) %}
    {% set owner = meta_dict.get('owner') %}
    {% set node_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(node_tags, meta_tags) %}
    {% set flatten_source_metadata_dict = {
         'unique_id': node_dict.get('unique_id'),
         'database_name': node_dict.get('database'),
         'schema_name': node_dict.get('schema'),
         'source_name': node_dict.get('source_name'),
         'name': node_dict.get('name'),
         'identifier': node_dict.get('identifier'),
         'loaded_at_field': node_dict.get('loaded_at_field'),
         'freshness_warn_after': freshness_dict.get('warn_after', {}),
         'freshness_error_after': freshness_dict.get('error_after', {}),
         'freshness_filter': freshness_dict.get('filter'),
         'relation_name': node_dict.get('relation_name'),
         'tags': tags,
         'meta': meta_dict,
         'owner': owner,
         'package_name': node_dict.get('package_name'),
         'original_path': node_dict.get('original_file_path'),
         'path': node_dict.get('path'),
         'source_description': node_dict.get('source_description'),
         'description': node_dict.get('description'),
         'generated_at': elementary.datetime_now_utc_as_string()
     }%}
    {{ return(flatten_source_metadata_dict) }}
{% endmacro %}