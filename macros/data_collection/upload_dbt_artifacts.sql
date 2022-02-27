{% macro upload_dbt_artifacts(results) %}
    -- depends_on: {{ ref('dbt_run_results') }}
    {%- set graph_var = graph %}
    {{ debug() }}
    {% if execute and results %}
        {% set run_result_flatten_dicts = [] %}
        {% for run_result in results -%}
            {% set flatten_run_result_dict = elementary_data_reliability.flatten_run_result(run_result) %}
            {% do run_result_flatten_dicts.append(flatten_run_result_dict) %}
        {% endfor %}
        {% do elementary_data_reliability.insert_dicts_to_table(ref('dbt_run_results'), run_result_flatten_dicts) %}
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
        'run_started_at': run_started_at.strftime('%Y-%m-%d %H:%M:%S'),
        'rows_affected': run_result_dict.get('adapter_response', {}).get('rows_affected'),
        'execution_time': run_result_dict.get('execution_time'),
        'status': run_result_dict.get('status'),
        'resource_type': node.get('resource_type'),
        'execute_started_at': none,
        'execute_completed_at': none,
        'compile_started_at': none,
        'compile_completed_at': none,
        'alias': node.get('alias'),
        'checksum': node.get('checksum', {}).get('checksum'),
        'materialization': node.get('config', {}).get('materialized'),
        'test_column_name': node.get('column_name'),
        'config_tags': node.get('config', {}).get('tags', []),
        'config_meta': node.get('config', {}).get('meta', {}),
        'tags': node.get('tags', []),
        'meta': node.get('meta', {}),
        'database_name': node.get('database'),
        'schema_name': node.get('schema'),
        'depends_on_macros': node.get('depends_on', {}).get('macros', []),
        'depends_on_nodes': node.get('depends_on', {}).get('nodes', []),
        'description': node.get('description'),
        'name': node.get('name'),
        'package_name': node.get('package_name'),
        'original_path': node.get('original_file_path'),
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
        'resource_type': node_dict.get('resource_type'),
        'alias': node_dict.get('alias'),
        'checksum': node_dict.get('checksum', {}).get('checksum'),
        'materialization': node_dict.get('config', {}).get('materialized'),
        'test_column_name': node_dict.get('column_name'),
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
        'original_path': node_dict.get('original_file_path')
    }%}
{% endmacro %}

{% macro flatten_test_metadata(node_dict) %}
    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'resource_type': node_dict.get('resource_type'),
        'alias': node_dict.get('alias'),
        'checksum': node_dict.get('checksum', {}).get('checksum'),
        'materialization': node_dict.get('config', {}).get('materialized'),
        'test_column_name': node_dict.get('column_name'),
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
        'original_path': node_dict.get('original_file_path')
    }%}
{% endmacro %}


{% macro flatten_source_metadata(node_dict) %}
{% endmacro %}

{% macro flatten_exposure_metadata(node_dict) %}
{% endmacro %}

{% macro flatten_metrics_metadata(node_dict) %}
{% endmacro %}

{# TODO: separate run results and test results #}
