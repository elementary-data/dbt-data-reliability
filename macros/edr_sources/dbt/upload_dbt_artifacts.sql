{% macro upload_dbt_artifacts(results) %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run %}
        -- handle models
        {% set nodes = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {% set flatten_node_macro = context['elementary']['flatten_model'] %}
        {% set dbt_models_empty_table_query = elementary.get_dbt_models_empty_table_query() %}
        {% set dbt_models = elementary.create_source_table('dbt_models', dbt_models_empty_table_query, True) %}
        {% do elementary.insert_nodes_to_table(dbt_models, nodes, flatten_node_macro) %}

        -- handle tests
        {% set nodes = graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% set flatten_node_macro = context['elementary']['flatten_test'] %}
        {% set dbt_tests_empty_table_query = elementary.get_dbt_tests_empty_table_query() %}
        {% set dbt_tests = elementary.create_source_table('dbt_tests', dbt_tests_empty_table_query, True) %}
        {% do elementary.insert_nodes_to_table(dbt_tests, nodes, flatten_node_macro) %}

        -- handle sources
        {% set nodes = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {% set flatten_node_macro = context['elementary']['flatten_source'] %}
        {% set dbt_sources_empty_table_query = elementary.get_dbt_sources_empty_table_query() %}
        {% set dbt_sources = elementary.create_source_table('dbt_sources', dbt_sources_empty_table_query, True) %}
        {% do elementary.insert_nodes_to_table(dbt_sources, nodes, flatten_node_macro) %}

        -- handle exposures
        {% set nodes = graph.exposures.values() | selectattr('resource_type', '==', 'exposure') %}
        {% set flatten_node_macro = context['elementary']['flatten_exposure'] %}
        {% set dbt_exposures_empty_table_query = elementary.get_dbt_exposures_empty_table_query() %}
        {% set dbt_exposures = elementary.create_source_table('dbt_exposures', dbt_exposures_empty_table_query, True) %}
        {% do elementary.insert_nodes_to_table(dbt_exposures, nodes, flatten_node_macro) %}

        -- handle metrics
        {% set nodes = graph.metrics.values() | selectattr('resource_type', '==', 'metric') %}
        {% set flatten_node_macro = context['elementary']['flatten_metric'] %}
        {% set dbt_metrics_empty_table_query = elementary.get_dbt_metrics_empty_table_query() %}
        {% set dbt_metrics = elementary.create_source_table('dbt_metrics', dbt_metrics_empty_table_query, True) %}
        {% do elementary.insert_nodes_to_table(dbt_metrics, nodes, flatten_node_macro) %}

        -- handle run_results
        {% if results %}
            {% set flatten_node_macro = context['elementary']['flatten_run_result'] %}
            {% set dbt_run_results_empty_table_query = elementary.get_dbt_run_results_empty_table_query() %}
            {% set dbt_run_results = elementary.create_source_table('dbt_run_results', dbt_run_results_empty_table_query, False) %}
            {% do elementary.insert_nodes_to_table(dbt_run_results, results, flatten_node_macro) %}
        {% endif %}
    {% endif %}
    {{ return ('') }}
{% endmacro %}

{% macro insert_nodes_to_table(table_name, nodes, flatten_node_macro) %}
    {% set artifacts = [] %}
    {% for node in nodes %}
        {% set metadata_dict = flatten_node_macro(node) %}
        {% if metadata_dict is not none %}
            {% do artifacts.append(metadata_dict) %}
        {% endif %}
    {% endfor %}
    {% if artifacts | length > 0 %}
        {% do elementary.insert_dicts(table_name, artifacts) %}
    {% endif %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_name) %}
{% endmacro %}

{% macro get_dbt_run_results_empty_table_query() %}
    {% set dbt_run_results_empty_table_query = elementary.empty_table([('model_execution_id', 'string'),
                                                                       ('unique_id', 'string'),
                                                                       ('invocation_id', 'string'),
                                                                       ('generated_at', 'string'),
                                                                       ('name', 'string'),
                                                                       ('status', 'string'),
                                                                       ('resource_type', 'string'),
                                                                       ('execution_time', 'float'),
                                                                       ('execute_started_at', 'string'),
                                                                       ('execute_completed_at', 'string'),
                                                                       ('compile_started_at', 'string'),
                                                                       ('compile_completed_at', 'string'),
                                                                       ('rows_affected', 'int'),
                                                                       ('full_refresh', 'boolean')]) %}
    {{ return(dbt_run_results_empty_table_query) }}
{% endmacro %}

{% macro flatten_run_result(run_result) %}
    {% set run_result_dict = run_result.to_dict() %}
    {% set node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
    {% set flatten_run_result_dict = {
        'model_execution_id': [invocation_id, node.get('unique_id')] | join('.'),
        'invocation_id': invocation_id,
        'unique_id': node.get('unique_id'),
        'name': node.get('name'),
        'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
        'rows_affected': run_result_dict.get('adapter_response', {}).get('rows_affected'),
        'execution_time': run_result_dict.get('execution_time'),
        'status': run_result_dict.get('status'),
        'resource_type': node.get('resource_type'),
        'execute_started_at': none,
        'execute_completed_at': none,
        'compile_started_at': none,
        'compile_completed_at': none,
        'full_refresh': flags.FULL_REFRESH
    }%}

    {% set timings = elementary.safe_get_with_default(run_result_dict, 'timing', []) %}
    {% if timings %}
        {% for timing in timings %}
            {% if timing is mapping %}
                {% if timing.get('name') == 'execute' %}
                    {% do flatten_run_result_dict.update({'execute_started_at': timing.get('started_at'), 'execute_completed_at': timing.get('completed_at')}) %}
                {% elif timing.get('name') == 'compile' %}
                    {% do flatten_run_result_dict.update({'compile_started_at': timing.get('started_at'), 'compile_completed_at': timing.get('completed_at')}) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(flatten_run_result_dict) }}
{% endmacro %}

{% macro get_dbt_models_empty_table_query() %}
    {% set dbt_models_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                  ('alias', 'string'),
                                                                  ('checksum', 'string'),
                                                                  ('materialization', 'string'),
                                                                  ('tags', 'string'),
                                                                  ('meta', 'string'),
                                                                  ('database_name', 'string'),
                                                                  ('schema_name', 'string'),
                                                                  ('depends_on_macros', 'string'),
                                                                  ('depends_on_nodes', 'string'),
                                                                  ('description', 'string'),
                                                                  ('name', 'string'),
                                                                  ('package_name', 'string'),
                                                                  ('original_path', 'string'),
                                                                  ('path', 'string'),
                                                                  ('generated_at', 'string')]) %}
    {{ return(dbt_models_empty_table_query) }}
{% endmacro %}

{% macro flatten_model(node_dict) %}
    {% set checksum_dict = elementary.safe_get_with_default(node_dict, 'checksum', {}) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(config_meta_dict) %}

    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}

    {% set flatten_model_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'alias': node_dict.get('alias'),
        'checksum': checksum_dict.get('checksum'),
        'materialization': config_dict.get('materialized'),
        'tags': tags,
        'meta': meta_dict,
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S')
    }%}
    {{ return(flatten_model_metadata_dict) }}
{% endmacro %}

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
                                                                 ('tags', 'string'),
                                                                 ('meta', 'string'),
                                                                 ('depends_on_macros', 'string'),
                                                                 ('depends_on_nodes', 'string'),
                                                                 ('description', 'string'),
                                                                 ('package_name', 'string'),
                                                                 ('original_path', 'string'),
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
    {% set tags = elementary.union_lists(config_tags, global_tags) %}

    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'short_name': node_dict.get('test_metadata', {}).get('name'),
        'alias': node_dict.get('alias'),
        'test_column_name': node_dict.get('column_name'),
        'severity': config_dict.get('severity'),
        'warn_if': config_dict.get('warn_if'),
        'error_if': config_dict.get('error_if'),
        'tags': tags,
        'meta': meta_dict,
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path'),
        'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S')
    }%}
    {{ return(flatten_test_metadata_dict) }}
{% endmacro %}

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
                                                                   ('freshness_filter', 'string'),
                                                                   ('relation_name', 'string'),
                                                                   ('tags', 'string'),
                                                                   ('meta', 'string'),
                                                                   ('package_name', 'string'),
                                                                   ('original_path', 'string'),
                                                                   ('path', 'string'),
                                                                   ('source_description', 'string'),
                                                                   ('description', 'string'),
                                                                   ('generated_at', 'string')]) %}
    {{ return(dbt_sources_empty_table_query) }}
{% endmacro %}

{% macro flatten_source(node_dict) %}
    {% set freshness_dict = elementary.safe_get_with_default(node_dict, 'freshness', {}) %}
    {% set source_meta_dict = elementary.safe_get_with_default(node_dict, 'source_meta', {}) %}
    {% set meta_dict = elementary.safe_get_with_default(node_dict, 'meta', {}) %}
    {% do meta_dict.update(source_meta_dict) %}
    {% set tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
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
         'package_name': node_dict.get('package_name'),
         'original_path': node_dict.get('original_file_path'),
         'path': node_dict.get('path'),
         'source_description': node_dict.get('source_description'),
         'description': node_dict.get('description'),
         'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S')
     }%}
    {{ return(flatten_source_metadata_dict) }}
{% endmacro %}

{% macro get_dbt_exposures_empty_table_query() %}
    {% set dbt_exposures_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                     ('name', 'string'),
                                                                     ('maturity', 'string'),
                                                                     ('type', 'string'),
                                                                     ('owner_email', 'string'),
                                                                     ('owner_name', 'string'),
                                                                     ('url', 'string'),
                                                                     ('depends_on_macros', 'string'),
                                                                     ('depends_on_nodes', 'string'),
                                                                     ('description', 'string'),
                                                                     ('tags', 'string'),
                                                                     ('meta', 'string'),
                                                                     ('package_name', 'string'),
                                                                     ('original_path', 'string'),
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
        'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S')
      }%}
    {{ return(flatten_exposure_metadata_dict) }}
{% endmacro %}

{% macro get_dbt_metrics_empty_table_query() %}
    {% set dbt_metrics_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                   ('name', 'string'),
                                                                   ('label', 'string'),
                                                                   ('model', 'string'),
                                                                   ('type', 'string'),
                                                                   ('sql', 'string'),
                                                                   ('timestamp', 'string'),
                                                                   ('filters', 'string'),
                                                                   ('time_grains', 'string'),
                                                                   ('dimensions', 'string'),
                                                                   ('depends_on_macros', 'string'),
                                                                   ('depends_on_nodes', 'string'),
                                                                   ('description', 'string'),
                                                                   ('tags', 'string'),
                                                                   ('meta', 'string'),
                                                                   ('package_name', 'string'),
                                                                   ('original_path', 'string'),
                                                                   ('path', 'string'),
                                                                   ('generated_at', 'string')]) %}
    {{ return(dbt_metrics_empty_table_query) }}
{% endmacro %}

{% macro flatten_metric(node_dict) %}
    {% set depends_on_dict = safe_get_with_default(node_dict, 'depends_on', {}) %}
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
        'generated_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S')
    }%}
    {{ return(flatten_metrics_metadata_dict) }}
{% endmacro %}
