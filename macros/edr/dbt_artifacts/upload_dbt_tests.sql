{%- macro upload_dbt_tests(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_tests') %}
    {% if execute and relation %}
        {% set tests = graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% do elementary.upload_artifacts_to_table(relation, tests, elementary.flatten_test, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}




{% macro get_dbt_tests_empty_table_query() %}
    {% set dbt_tests_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                 ('database_name', 'string'),
                                                                 ('schema_name', 'string'),
                                                                 ('name', 'string'),
                                                                 ('short_name', 'string'),
                                                                 ('alias', 'string'),
                                                                 ('test_column_name', 'string'),
                                                                 ('severity', 'string'),
                                                                 ('warn_if', 'string'),
                                                                 ('error_if', 'string'),
                                                                 ('test_params', 'long_string'),
                                                                 ('test_namespace', 'string'),
                                                                 ('tags', 'long_string'),
                                                                 ('model_tags', 'long_string'),
                                                                 ('model_owners', 'long_string'),
                                                                 ('meta', 'long_string'),
                                                                 ('depends_on_macros', 'long_string'),
                                                                 ('depends_on_nodes', 'long_string'),
                                                                 ('parent_model_unique_id', 'string'),
                                                                 ('description', 'long_string'),
                                                                 ('package_name', 'string'),
                                                                 ('type', 'string'),
                                                                 ('original_path', 'long_string'),
                                                                 ('path', 'string'),
                                                                 ('generated_at', 'string')]) %}
    {{ return(dbt_tests_empty_table_query) }}
{% endmacro %}

{% macro flatten_test(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(config_meta_dict) %}

    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}

    {% set test_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(node_dict) %}
    {% set test_model_nodes = elementary.get_nodes_by_unique_ids(test_model_unique_ids) %}
    {% set test_models_owners = [] %}
    {% set test_models_tags = [] %}
    {% for test_model_node in test_model_nodes %}
        {% set flatten_test_model_node = elementary.flatten_model(test_model_node) %}
        {% set test_model_owner = flatten_test_model_node.get('owner') %}
        {% if test_model_owner %}
            {% if test_model_owner is string %}
                {% do test_models_owners.append(test_model_owner) %}
            {% elif test_model_owner is iterable %}
                {% do test_models_owners.extend(test_model_owner) %}
            {% endif %}
        {% endif %}
        {% set test_model_tags = flatten_test_model_node.get('tags') %}
        {% if test_model_tags and test_model_tags is sequence %}
            {% do test_models_tags.extend(test_model_tags) %}
        {% endif %}
    {% endfor %}
    {% set test_models_owners = test_models_owners | unique | list %}
    {% set test_models_tags = test_models_tags | unique | list %}

    {% set primary_test_model_database, primary_test_model_schema = elementary.get_model_database_and_schema_from_test_node(node_dict) %}
    {% set test_metadata = elementary.safe_get_with_default(node_dict, 'test_metadata', {}) %}
    {% set test_kwargs = elementary.safe_get_with_default(test_metadata, 'kwargs', {}) %}
    {% set test_model_jinja = test_kwargs.get('model') %}
    {%- if test_model_unique_ids | length == 1 -%}
        {# if only one parent model for this test, simply use this model #}
        {% set primary_test_model_id = test_model_unique_ids[0] %}
    {%- else -%}
        {# if there are multiple parent models for a test, try finding it using the model jinja in the test graph node #}
        {% set primary_test_model_id = none %}
        {% if test_model_jinja %}
            {% set primary_test_model_candidates = [] %}
            {% for test_model_unique_id in test_model_unique_ids %}
                {% set split_test_model_unique_id = test_model_unique_id.split('.') %}
                {% if split_test_model_unique_id and split_test_model_unique_id | length > 0 %}
                    {% set test_model_name = split_test_model_unique_id[-1] %}
                    {% if test_model_name and test_model_name in test_model_jinja %}
                        {% do primary_test_model_candidates.append(test_model_unique_id) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
            {% if primary_test_model_candidates | length == 1 %}
                {% set primary_test_model_id = primary_test_model_candidates[0] %}
            {% endif %}
        {% endif %}
    {%- endif -%}
    {% set original_file_path = node_dict.get('original_file_path') %}
    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'short_name': test_metadata.get('name'),
        'alias': node_dict.get('alias'),
        'test_column_name': node_dict.get('column_name'),
        'severity': config_dict.get('severity'),
        'warn_if': config_dict.get('warn_if'),
        'error_if': config_dict.get('error_if'),
        'test_params': test_kwargs,
        'test_namespace': test_metadata.get('namespace'),
        'tags': tags,
        'model_tags': test_models_tags,
        'model_owners': test_models_owners,
        'meta': meta_dict,
        'database_name': primary_test_model_database,
        'schema_name': primary_test_model_schema,
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'parent_model_unique_id': primary_test_model_id,
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'type': elementary.get_test_type(original_file_path),
        'original_path': original_file_path,
        'compiled_code': elementary.get_compiled_code(node_dict),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string()
    }%}
    {{ return(flatten_test_metadata_dict) }}
{% endmacro %}

{% macro get_test_type(test_path) %}
    {% set test_type = 'generic' %}
    {%- if 'tests/generic' in test_path or 'macros/' in test_path -%}
        {% set test_type = 'generic' %}
    {%- elif 'tests/' in test_path -%}
        {% set test_type = 'singular' %}
    {%- endif -%}
    {{- return(test_type) -}}
{%- endmacro -%}