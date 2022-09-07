{% macro upload_run_results(results) %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run and results %}
        {% if elementary.get_config_var('disable_run_results') %}
            {% do elementary.edr_log("Run results are disabled, skipping upload.") %}
            {{ return('') }}
        {% endif %}
        {{ elementary.debug_log("Uploading run results.") }}
        {% set database_name, schema_name = elementary.get_model_database_and_schema('elementary', 'dbt_run_results') %}
        {%- set dbt_run_results_relation = adapter.get_relation(database=database_name,
                                                                schema=schema_name,
                                                                identifier='dbt_run_results') -%}
        {%- if dbt_run_results_relation -%}
            {% do elementary.upload_artifacts_to_table(dbt_run_results_relation, results, elementary.get_flatten_run_result_callback(),
                                                       should_commit=True) %}
        {%- endif -%}
    {% endif %}
    {{ elementary.edr_log("Uploaded run results successfully.") }}
    {{ return ('') }}
{% endmacro %}


{% macro get_dbt_run_results_empty_table_query() %}
    {% set dbt_run_results_empty_table_query = elementary.empty_table([('model_execution_id', 'long_string'),
                                                                       ('unique_id', 'long_string'),
                                                                       ('invocation_id', 'string'),
                                                                       ('generated_at', 'string'),
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
                                                                       ('compiled_sql', 'long_string')
                                                                       ]) %}
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
        'compiled_sql': node.get('compiled_sql')
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
