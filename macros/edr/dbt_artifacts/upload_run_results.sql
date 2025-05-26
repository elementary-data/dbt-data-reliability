{% macro upload_run_results() %}
    {% set relation = elementary.get_elementary_relation('dbt_run_results') %}
    {% if execute and relation %}
        {{ elementary.file_log("Uploading run results.") }}
        {% do elementary.upload_artifacts_to_table(relation, results, elementary.flatten_run_result, append=True, should_commit=True, on_query_exceed=elementary.on_run_result_query_exceed) %}
        {{ elementary.file_log("Uploaded run results successfully.") }}
    {% endif %}
    {{ return ('') }}
{% endmacro %}


{% macro get_dbt_run_results_empty_table_query() %}
    {% set dbt_run_results_empty_table_query = elementary.empty_table([('model_execution_id', 'long_string'),
                                                                       ('unique_id', 'long_string'),
                                                                       ('invocation_id', 'string'),
                                                                       ('generated_at', 'string'),
                                                                       ('created_at', 'timestamp'),
                                                                       ('name', 'long_string'),
                                                                       ('message', 'long_string'),
                                                                       ('status', 'string'),
                                                                       ('resource_type', 'string'),
                                                                       ('execution_time', 'float'),
                                                                       ('execute_started_at', 'string'),
                                                                       ('execute_completed_at', 'string'),
                                                                       ('compile_started_at', 'string'),
                                                                       ('compile_completed_at', 'string'),
                                                                       ('rows_affected', 'bigint'),
                                                                       ('full_refresh', 'boolean'),
                                                                       ('compiled_code', 'long_string'),
                                                                       ('failures', 'bigint'),
                                                                       ('query_id', 'string'),
                                                                       ('thread_id', 'string'),
                                                                       ('materialization', 'string'),
                                                                       ('adapter_response', 'string'),
                                                                       ('group_name', 'string'),
                                                                       ]) %}
    {{ return(dbt_run_results_empty_table_query) }}
{% endmacro %}

{% macro flatten_run_result(run_result) %}
    {% set run_result_dict = elementary.get_run_result_dict(run_result) %}
    {% set node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
    {% set config_dict = elementary.safe_get_with_default(node, 'config', {}) %}
    {% set flatten_run_result_dict = {
        'model_execution_id': elementary.get_node_execution_id(node),
        'invocation_id': invocation_id,
        'unique_id': node.get('unique_id'),
        'name': node.get('name'),
        'message': run_result_dict.get('message'),
        'generated_at': elementary.datetime_now_utc_as_string(),
        'rows_affected': run_result_dict.get('adapter_response', {}).get('rows_affected'),
        'execution_time': run_result_dict.get('execution_time'),
        'status': run_result_dict.get('status'),
        'resource_type': node.get('resource_type'),
        'execute_started_at': none,
        'execute_completed_at': none,
        'compile_started_at': none,
        'compile_completed_at': none,
        'full_refresh': flags.FULL_REFRESH,
        'compiled_code': elementary.get_compiled_code(node, as_column_value=true),
        'failures': run_result_dict.get('failures'),
        'query_id': run_result_dict.get('adapter_response', {}).get('query_id'),
        'thread_id': run_result_dict.get('thread_id'),
        'materialization': config_dict.get('materialized'),
        'adapter_response': run_result_dict.get('adapter_response', {}),
        'group_name': config_dict.get('group'),
    } %}

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

{% macro on_run_result_query_exceed(flattened_node) %}
    {% do flattened_node.update({"compiled_code": elementary.get_compiled_code_too_long_err_msg()}) %}
{% endmacro %}
