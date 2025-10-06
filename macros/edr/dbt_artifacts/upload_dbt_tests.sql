{%- macro upload_dbt_tests(should_commit=false, metadata_hashes=none) -%}
    {% set relation = elementary.get_elementary_relation('dbt_tests') %}
    {% if execute and relation %}
        {% set tests = graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% do elementary.upload_artifacts_to_table(relation, tests, elementary.flatten_test, should_commit=should_commit, metadata_hashes=metadata_hashes) %}
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
                                                                 ('test_original_name', 'string'),
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
                                                                 ('generated_at', 'string'),
                                                                 ('metadata_hash', 'string'),
                                                                 ('quality_dimension', 'string'),
                                                                 ('group_name', 'string'),
                                                                 ]) %}
    {{ return(dbt_tests_empty_table_query) }}
{% endmacro %}

{% macro flatten_test(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set test_metadata = elementary.safe_get_with_default(node_dict, 'test_metadata', {}) %}
    {% set test_namespace = test_metadata.get('namespace') %}
    {% set test_original_name = test_metadata.get('name') %}
    {% set test_short_name = elementary.get_test_short_name(node_dict, test_metadata) %}

    {% set default_description = elementary.get_default_description(test_original_name, test_namespace) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}

    {% set unified_meta = {} %}
    {% do unified_meta.update(config_meta_dict) %}
    {% do unified_meta.update(meta_dict) %}

    {% set description = none %}
    {% if dbt_version >= '1.9.0' and node_dict.get('description') %}
        {% set description = node_dict.get('description') %}
    {% elif unified_meta.get('description') %}
        {% set description = unified_meta.pop('description') %}
    {% elif default_description %}
        {% set description = default_description %}
    {% endif %}

    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(unified_meta, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}

    {% set test_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(node_dict) %}
    {% set test_model_nodes = elementary.get_nodes_by_unique_ids(test_model_unique_ids) %}
    {% set test_models_owners = [] %}
    {% set test_models_tags = [] %}
    {% for test_model_node in test_model_nodes %}
        {% set flatten_test_model_node = elementary.flatten_node(test_model_node) %}
        {% set test_model_owner = flatten_test_model_node.get('owner') %}
        {% if test_model_owner %}
            {% if test_model_owner is string %}
                {% set owners = test_model_owner.split(',') %}
                {% for owner in owners %}
                    {% do test_models_owners.append(owner | trim) %}  
                {% endfor %}
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

    {% set test_kwargs = elementary.safe_get_with_default(test_metadata, 'kwargs', {}) %}

    {% set primary_test_model_id = namespace(data=none) %}
    {% set override_primary_test_model_id = elementary.safe_get_with_default(config_dict, 'override_primary_test_model_id')%}

    {% if override_primary_test_model_id %}
        {% set primary_test_model_id.data = override_primary_test_model_id %}
    {% elif test_model_unique_ids | length == 1 %}
        {# if only one parent model for this test, simply use this model #}
        {% set primary_test_model_id.data = test_model_unique_ids[0] %}
    {% else %}
      {% set test_model_jinja = test_kwargs.get('model') %}
      {% if test_model_jinja %}
        {% set test_model_name_matches =
            modules.re.findall("ref\(['\"](\w+)['\"]\)", test_model_jinja) +
            modules.re.findall("source\(['\"]\w+['\"], ['\"](\w+)['\"]\)", test_model_jinja) %}
        {% if test_model_name_matches | length == 1 %}
          {% set test_model_name = test_model_name_matches[0] %}
          {% for test_model_unique_id in test_model_unique_ids %}
              {% set split_test_model_unique_id = test_model_unique_id.split('.') %}
              {% if split_test_model_unique_id and split_test_model_unique_id | length > 0 %}
                  {% set test_node_model_name = split_test_model_unique_id[-1] %}
                  {% if test_node_model_name == test_model_name %}
                    {% set primary_test_model_id.data = test_model_unique_id %}
                  {% endif %}
              {% endif %}
          {% endfor %}
        {% endif %}
      {% endif %}
    {% endif %}

    {% set group_name = config_dict.get("group") or node_dict.get("group") %}

    {% set primary_test_model_database = none %}
    {% set primary_test_model_schema = none %}
    {%- if primary_test_model_id.data is not none -%}
        {% set tested_model_node = elementary.get_node(primary_test_model_id.data) %}
        {%- if tested_model_node -%}
            {% set primary_test_model_database = tested_model_node.get('database') %}
            {% set primary_test_model_schema = tested_model_node.get('schema') %}
            {% set group_name = group_name or tested_model_node.get('group') %}
        {%- endif -%}
    {%- endif -%}

    {%- if primary_test_model_database is none or primary_test_model_schema is none -%}
        {# This is mainly here to support singular test cases with multiple referred models, in this case the tested node is being used to extract the db and schema #}
        {% set primary_test_model_database, primary_test_model_schema = elementary.get_model_database_and_schema_from_test_node(node_dict) %}
    {%- endif -%}

    {% set original_file_path = node_dict.get('original_file_path') %}
    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'short_name': test_short_name,
        'alias': node_dict.get('alias'),
        'test_column_name': node_dict.get('column_name') or test_kwargs.get('column_name'),
        'severity': config_dict.get('severity'),
        'warn_if': config_dict.get('warn_if'),
        'error_if': config_dict.get('error_if'),
        'test_params': test_kwargs,
        'test_namespace': test_namespace,
        'test_original_name': test_original_name,
        'tags': elementary.filter_none_and_sort(tags),
        'model_tags': elementary.filter_none_and_sort(test_models_tags),
        'model_owners': elementary.filter_none_and_sort(test_models_owners),
        'meta': unified_meta,
        'database_name': primary_test_model_database,
        'schema_name': primary_test_model_schema,
        'depends_on_macros': elementary.filter_none_and_sort(depends_on_dict.get('macros', [])),
        'depends_on_nodes': elementary.filter_none_and_sort(depends_on_dict.get('nodes', [])),
        'parent_model_unique_id': primary_test_model_id.data,
        'description': description,
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'type': elementary.get_test_sub_type(original_file_path, test_namespace),
        'original_path': original_file_path,
        'compiled_code': elementary.get_compiled_code(node_dict),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string(),
        'quality_dimension': unified_meta.get('quality_dimension') or elementary.get_quality_dimension(test_original_name, test_namespace),
        'group_name': group_name,
    }%}
    {% do flatten_test_metadata_dict.update({"metadata_hash": elementary.get_artifact_metadata_hash(flatten_test_metadata_dict)}) %}
    {{ return(flatten_test_metadata_dict) }}
{% endmacro %}

{% macro get_test_sub_type(test_path, test_namespace = none) %}
    {% set test_type = 'generic' %}
    {%- if test_namespace == 'dbt_expectations' -%}
        {% set test_type = 'expectation' %}
    {%- elif 'tests/generic' in test_path or 'macros/' in test_path or 'generic_tests/' in test_path -%}
        {% set test_type = 'generic' %}
    {%- elif 'tests/' in test_path -%}
        {% set test_type = 'singular' %}
    {%- endif -%}
    {{- return(test_type) -}}
{%- endmacro -%}


{% macro get_test_short_name(node_dict, test_metadata) %}
    {#
    If there is a custom name it overrides the dbt auto generated long name.
    This is a best effort to extract custom names.
    dbt automated name generation -
        - Generic test long name starts with the generic name or source_generic
        - Generic tests from packages long name starts with the package_generic or package_source_generic
    #}

    {% set generic_test_name = test_metadata.get('name') %} {# 'unique', 'relationships', 'volume_anomalies' etc #}
    {% set test_package_name = test_metadata.get('namespace') %}
    {% set test_instance_name = node_dict.get('name') %} {# Test custom name or dbt auto generated long name #}
    {%- if generic_test_name %}
        {%- if test_package_name == 'elementary' %}
            {{ return(generic_test_name) }}
        {%- elif test_package_name %}
            {% set test_short_name =
                generic_test_name if (test_instance_name.startswith(test_package_name + '_' + generic_test_name) or test_instance_name.startswith(test_package_name + '_source_' + generic_test_name))
                else test_instance_name
            %}
        {%- else %}
            {% set test_short_name =
                generic_test_name if (test_instance_name.startswith(generic_test_name) or test_instance_name.startswith('source_' + generic_test_name))
                else test_instance_name
            %}
        {%- endif %}
        {{ return(test_short_name) }}
    {%- else %}
        {{ return(test_instance_name) }}
    {%- endif %}
{% endmacro %}


{% macro get_default_description(test_original_name, test_namespace = none) %}
    {% set description = none %}
    {% set common_test_config = elementary.get_common_test_config_by_namespace_and_name(test_namespace, test_original_name) %}
    {% if common_test_config %}
        {% set description = common_test_config.get("description") %}
    {% endif %}
    {% do return(description) %}
{% endmacro %}


{% macro get_quality_dimension(test_original_name, test_namespace = none) %}
    {% set quality_dimension = none %}
    {% set common_test_config = elementary.get_common_test_config_by_namespace_and_name(test_namespace, test_original_name) %}
    {% if common_test_config %}
        {% set quality_dimension = common_test_config.get("quality_dimension") %}
    {% endif %}
    {% do return(quality_dimension) %}
{% endmacro %}
