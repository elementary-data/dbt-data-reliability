{% macro upload_dbt_artifacts(results) %}
    -- depends_on: {{ ref('dbt_models') }}
    -- depends_on: {{ ref('dbt_tests') }}
    -- depends_on: {{ ref('dbt_sources') }}
    -- depends_on: {{ ref('dbt_exposures') }}
    -- depends_on: {{ ref('dbt_metrics') }}
    -- depends_on: {{ ref('dbt_run_results') }}
    {% if execute %}
        {% set nodes = graph.nodes.values() %}
        -- handle models
        {% set flatten_model_artifacts = [] %}
        {% for node in nodes | selectattr('resource_type', '==', 'model') %}
            {% set flatten_model_metadata = elementary_data_reliability.flatten_model_metadata(node) %}
            {% do flatten_model_artifacts.append(flatten_model_metadata) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_models'), flatten_model_artifacts) %}

        -- handle tests
        {% set flatten_test_artifacts = [] %}
        {% for node in nodes | selectattr('resource_type', '==', 'test') %}
            {% set flatten_test_metadata = elementary_data_reliability.flatten_test_metadata(node) %}
            {% do flatten_test_artifacts.append(flatten_test_metadata) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_tests'), flatten_test_artifacts) %}

        -- handle sources
        {% set nodes = graph.sources.values() %}
        {% set flatten_source_artifacts = [] %}
        {% for node in nodes | selectattr('resource_type', '==', 'source') %}
            {% set flatten_source_metadata = elementary_data_reliability.flatten_source_metadata(node) %}
            {% do flatten_source_artifacts.append(flatten_source_metadata) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_sources'), flatten_source_artifacts) %}

        -- handle exposures
        {% set nodes = graph.exposures.values() %}
        {% set flatten_exposure_artifacts = [] %}
        {% for node in nodes | selectattr('resource_type', '==', 'exposure') %}
            {% set flatten_exposure_metadata = elementary_data_reliability.flatten_exposure_metadata(node) %}
            {% do flatten_exposure_artifacts.append(flatten_exposure_metadata) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_exposures'), flatten_exposure_artifacts) %}

        -- handle metrics
        {% set nodes = graph.metrics.values() %}
        {% set flatten_metric_artifacts = [] %}
        {% for node in nodes | selectattr('resource_type', '==', 'metric') %}
            {% set flatten_metric_metadata = elementary_data_reliability.flatten_metric_metadata(node) %}
            {% do flatten_metric_artifacts.append(flatten_metric_metadata) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_metrics'), flatten_metric_artifacts) %}

        -- handle run_results
        {% if results %}
                {% set run_result_flatten_dicts = [] %}
                {% for run_result in results -%}
                    {% set flatten_run_result_dict = elementary_data_reliability.flatten_run_result(run_result) %}
                    {% do run_result_flatten_dicts.append(flatten_run_result_dict) %}
                {% endfor %}
                {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_run_results'), run_result_flatten_dicts) %}
            {% endif %}
        {% endif %}
    {{ return ('') }}
{% endmacro %}

{% macro flatten_run_result(run_result) %}
    {% set run_result_dict = run_result.to_dict() %}
    {% set node = run_result_dict.get('node', {}) %}
    {% set flatten_run_result_dict = {
        'model_execution_id': [invocation_id, node.get('unique_id')] | join('.'),
        'invocation_id': invocation_id,
        'unique_id': node.get('unique_id'),
        'name': node.get('name'),
        'run_started_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
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

    {% set timings = run_result_dict.get('timing', []) %}
    {% for timing in timings %}
        {% if timing is mapping %}
            {% if timing.get('name') == 'execute' %}
                {% do flatten_run_result_dict.update({'execute_started_at': timing.get('started_at'), 'execute_completed_at': timing.get('completed_at')}) %}
            {% elif timing.get('name') == 'compile' %}
                {% do flatten_run_result_dict.update({'compile_started_at': timing.get('started_at'), 'compile_completed_at': timing.get('completed_at')}) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ return(flatten_run_result_dict) }}
{% endmacro %}



{% macro flatten_model_metadata(node_dict) %}
    {% set flatten_model_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'alias': node_dict.get('alias'),
        'checksum': node_dict.get('checksum', {}).get('checksum'),
        'materialization': node_dict.get('config', {}).get('materialized'),
        'config_tags': node_dict.get('config', {}).get('tags', []),
        'config_meta': node_dict.get('config', {}).get('meta', {}),
        'tags': node_dict.get('tags', []),
        'meta': node_dict.get('meta', {}),
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'depends_on_macros': node_dict.get('depends_on', {}).get('macros', []),
        'depends_on_nodes': node_dict.get('depends_on', {}).get('nodes', []),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path')
    }%}
    {{ return(flatten_model_metadata_dict) }}
{% endmacro %}

{% macro flatten_test_metadata(node_dict) %}
    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'short_name': node_dict.get('test_metadata', {}).get('name'),
        'alias': node_dict.get('alias'),
        'test_column_name': node_dict.get('column_name'),
        'severity': node_dict.get('config', {}).get('severity'),
        'warn_if': node_dict.get('config', {}).get('warn_if'),
        'error_if': node_dict.get('config', {}).get('error_if'),
        'config_tags': node_dict.get('config', {}).get('tags', []),
        'config_meta': node_dict.get('config', {}).get('meta', {}),
        'tags': node_dict.get('tags', []),
        'meta': node_dict.get('meta', {}),
        'database_name': node_dict.get('database'),
        'schema_name': node_dict.get('schema'),
        'depends_on_macros': node_dict.get('depends_on', {}).get('macros', []),
        'depends_on_nodes': node_dict.get('depends_on', {}).get('nodes', []),
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path')
    }%}
    {{ return(flatten_test_metadata_dict) }}
{% endmacro %}


{% macro flatten_source_metadata(node_dict) %}
    {% set flatten_source_metadata_dict = {
         'unique_id': node_dict.get('unique_id'),
         'database_name': node_dict.get('database'),
         'schema_name': node_dict.get('schema'),
         'source_name': node_dict.get('source_name'),
         'name': node_dict.get('name'),
         'identifier': node_dict.get('identifier'),
         'loaded_at_field': node_dict.get('loaded_at_field'),
         'freshness_warn_after': node_dict.get('freshness', {}).get('warn_after', {}),
         'freshness_error_after': node_dict.get('freshness', {}).get('error_after', {}),
         'freshness_filter': node_dict.get('freshness', {}).get('filter'),
         'relation_name': node_dict.get('relation_name'),
         'source_meta': node_dict.get('source_meta'),
         'tags': node_dict.get('tags', []),
         'meta': node_dict.get('meta', {}),
         'package_name': node_dict.get('package_name'),
         'original_path': node_dict.get('original_file_path'),
         'path': node_dict.get('path'),
         'source_description': node_dict.get('source_description'),
         'description': node_dict.get('description'),
     }%}
    {{ return(flatten_source_metadata_dict) }}
{% endmacro %}

{% macro flatten_exposure_metadata(node_dict) %}
    {% set flatten_exposure_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'name': node_dict.get('name'),
        'maturity': node_dict.get('maturity'),
        'type': node_dict.get('type'),
        'owner_email': node_dict.get('owner', {}).get('email'),
        'owner_name': node_dict.get('owner', {}).get('name'),
        'url': node_dict.get('url'),
        'depends_on_macros': node_dict.get('depends_on', {}).get('macros', []),
        'depends_on_nodes': node_dict.get('depends_on', {}).get('nodes', []),
        'description': node_dict.get('description'),
        'tags': node_dict.get('tags', []),
        'meta': node_dict.get('meta', {}),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path')
      }%}
    {{ return(flatten_exposure_metadata_dict) }}
{% endmacro %}

{% macro flatten_metric_metadata(node_dict) %}
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
        'depends_on_macros': node_dict.get('depends_on', {}).get('macros', []),
        'depends_on_nodes': node_dict.get('depends_on', {}).get('nodes', []),
        'description': node_dict.get('description'),
        'tags': node_dict.get('tags', []),
        'meta': node_dict.get('meta', {}),
        'package_name': node_dict.get('package_name'),
        'original_path': node_dict.get('original_file_path'),
        'path': node_dict.get('path')
    }%}
    {{ return(flatten_metrics_metadata_dict) }}
{% endmacro %}


{# TODO: separate run results and test results #}
