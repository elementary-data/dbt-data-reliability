{% macro upload_dbt_artifacts(results) %}
    -- depends_on: {{ ref('dbt_models') }}
    -- depends_on: {{ ref('dbt_tests') }}
    -- depends_on: {{ ref('dbt_sources') }}
    -- depends_on: {{ ref('dbt_exposures') }}
    -- depends_on: {{ ref('dbt_metrics') }}
    -- depends_on: {{ ref('dbt_run_results') }}
    {% if execute %}
        -- handle models
        {% set nodes = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {% set flatten_node_macro = context['elementary']['flatten_model'] %}
        {% do elementary.insert_nodes_to_table(ref('dbt_models'), nodes, flatten_node_macro) %}

        -- handle tests
        {% set nodes = graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% set flatten_node_macro = context['elementary']['flatten_test'] %}
        {% do elementary.insert_nodes_to_table(ref('dbt_tests'), nodes, flatten_node_macro) %}

        -- handle sources
        {% set nodes = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {% set flatten_node_macro = context['elementary']['flatten_source'] %}
        {% do elementary.insert_nodes_to_table(ref('dbt_sources'), nodes, flatten_node_macro) %}

        -- handle exposures
        {% set nodes = graph.exposures.values() | selectattr('resource_type', '==', 'exposure') %}
        {% set flatten_node_macro = context['elementary']['flatten_exposure'] %}
        {% do elementary.insert_nodes_to_table(ref('dbt_exposures'), nodes, flatten_node_macro) %}

        -- handle metrics
        {% set nodes = graph.metrics.values() | selectattr('resource_type', '==', 'metric') %}
        {% set flatten_node_macro = context['elementary']['flatten_metric'] %}
        {% do elementary.insert_nodes_to_table(ref('dbt_metrics'), nodes, flatten_node_macro) %}

        -- handle run_results
        {% if results %}
            {% set flatten_node_macro = context['elementary']['flatten_run_result'] %}
            {% do elementary.insert_nodes_to_table(ref('dbt_run_results'), results, flatten_node_macro) %}
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
        {% do elementary.insert_dicts_to_table(table_name, artifacts) %}
    {% endif %}
    -- remove empty rows created by dbt's materialization
    {% do elementary.remove_empty_rows(table_name) %}
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

{% macro flatten_model(node_dict) %}
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

{% macro flatten_test(node_dict) %}
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

{% macro flatten_source(node_dict) %}
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

{% macro flatten_exposure(node_dict) %}
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

{% macro flatten_metric(node_dict) %}
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

