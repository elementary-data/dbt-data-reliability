{% macro upload_dbt_artifacts(results) %}
        {% do elementary.edr_log("Deprecated - Please remove the call to elementary.upload_dbt_artifacts() on your on-run-end hook.") %}
{% endmacro %}


{% macro get_dbt_run_results_empty_table_query() %}
    {% set dbt_run_results_empty_table_query = elementary.empty_table([('model_execution_id', 'long_string'),
                                                                       ('unique_id', 'long_string'),
                                                                       ('invocation_id', 'string'),
                                                                       ('generated_at', 'string'),
                                                                       ('name', 'long_string'),
                                                                       ('status', 'string'),
                                                                       ('resource_type', 'string'),
                                                                       ('execution_time', 'float'),
                                                                       ('execute_started_at', 'string'),
                                                                       ('execute_completed_at', 'string'),
                                                                       ('compile_started_at', 'string'),
                                                                       ('compile_completed_at', 'string'),
                                                                       ('rows_affected', 'bigint'),
                                                                       ('full_refresh', 'boolean')]) %}
    {{ return(dbt_run_results_empty_table_query) }}
{% endmacro %}

{%- macro get_flatten_run_result_callback() -%}
    {{- return(adapter.dispatch('flatten_run_result', 'elementary')) -}}
{%- endmacro -%}

{%- macro flatten_run_result(node_dict) -%}
    {{- return(adapter.dispatch('flatten_run_result', 'elementary')(node_dict)) -}}
{%- endmacro -%}

{% macro default__flatten_run_result(run_result) %}
    {% set run_result_dict = run_result.to_dict() %}
    {% set node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
    {% set flatten_run_result_dict = {
        'model_execution_id': elementary.get_node_execution_id(node),
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

