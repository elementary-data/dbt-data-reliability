{%- macro upload_dbt_exposures(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_exposures') %}
    {% if execute and relation %}
        {% set exposures = graph.exposures.values() | selectattr('resource_type', '==', 'exposure') %}
        {% do elementary.upload_artifacts_to_table(relation, exposures, elementary.flatten_exposure, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}



{% macro get_dbt_exposures_empty_table_query() %}
    {% set dbt_exposures_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                     ('name', 'string'),
                                                                     ('maturity', 'string'),
                                                                     ('type', 'string'),
                                                                     ('owner_email', 'string'),
                                                                     ('owner_name', 'string'),
                                                                     ('url', 'long_string'),
                                                                     ('depends_on_macros', 'long_string'),
                                                                     ('depends_on_nodes', 'long_string'),
                                                                     ('description', 'long_string'),
                                                                     ('tags', 'long_string'),
                                                                     ('meta', 'long_string'),
                                                                     ('package_name', 'string'),
                                                                     ('original_path', 'long_string'),
                                                                     ('path', 'string'),
                                                                     ('generated_at', 'string')]) %}
    {{ return(dbt_exposures_empty_table_query) }}
{% endmacro %}

{% macro flatten_exposure(node_dict) %}
    {% set owner_dict = elementary.safe_get_with_default(node_dict, 'owner', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% set tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set flatten_exposure_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'name': node_dict.get('name'),
        'maturity': node_dict.get('maturity'),
        'type': node_dict.get('type'),
        'owner_email': owner_dict.get('email'),
        'owner_name': owner_dict.get('name'),
        'url': node_dict.get('url'),
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
    {{ return(flatten_exposure_metadata_dict) }}
{% endmacro %}