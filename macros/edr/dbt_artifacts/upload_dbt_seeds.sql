{%- macro upload_dbt_seeds(should_commit=false, metadata_hashes=none) -%}
    {% set relation = elementary.get_elementary_relation('dbt_seeds') %}
    {% if execute and relation %}
        {% set seeds = graph.nodes.values() | selectattr('resource_type', '==', 'seed') %}
        {% do elementary.upload_artifacts_to_table(relation, seeds, elementary.flatten_seed, should_commit=should_commit, metadata_hashes=metadata_hashes) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}

{% macro get_dbt_seeds_empty_table_query() %}
    {% set dbt_seeds_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                  ('alias', 'string'),
                                                                  ('checksum', 'string'),
                                                                  ('tags', 'long_string'),
                                                                  ('meta', 'long_string'),
                                                                  ('owner', 'string'),
                                                                  ('database_name', 'string'),
                                                                  ('schema_name', 'string'),
                                                                  ('description', 'long_string'),
                                                                  ('name', 'string'),
                                                                  ('package_name', 'string'),
                                                                  ('original_path', 'long_string'),
                                                                  ('path', 'string'),
                                                                  ('generated_at', 'string'),
                                                                  ('metadata_hash', 'string'),
                                                                  ('group_name', 'string'),
                                                                  ]) %}
    {{ return(dbt_seeds_empty_table_query) }}
{% endmacro %}

{% macro flatten_seed(node_dict) %}
    {% set checksum_dict = elementary.safe_get_with_default(node_dict, 'checksum', {}) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(config_meta_dict) %}
    {% set formatted_owner = [] %}
    {% set raw_owner = meta_dict.get('owner') or config_dict.get('owner') %}
    {% if raw_owner is string %}
        {% set owners = raw_owner.split(',') %}
        {% for owner in owners %}
            {% do formatted_owner.append(owner | trim) %}
        {% endfor %}
    {% elif raw_owner is iterable %}
        {% do formatted_owner.extend(raw_owner) %}
    {% endif %}
    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}

    {% set flatten_seed_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'alias': node_dict.get('alias'),
        'checksum': checksum_dict.get('checksum'),
        'tags': elementary.filter_none_and_sort(tags),
        'meta': meta_dict,
        'owner': elementary.filter_none_and_sort(formatted_owner),
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string(),
        'group_name': config_dict.get("group") or node_dict.get("group"),
    }%}
    {% do flatten_seed_metadata_dict.update({"metadata_hash": elementary.get_artifact_metadata_hash(flatten_seed_metadata_dict)}) %}
    {{ return(flatten_seed_metadata_dict) }}
{% endmacro %}