{% macro get_run_result_dict(run_result) %}
    {% set major, minor, revision = dbt_version.split(".") %}
    {% set major = major | int %}
    {% set minor = minor | int %}
    {% if major < 1 or major == 1 and minor < 8 %}
        {% do return(run_result.to_dict()) %}
    {% else %}
        {# There's a bug in dbt 1.8 which causes run_result.to_dict to fail on an exception #}
        {% do return({
            'status': run_result.status,
            'message': run_result.message,
            'adapter_response': run_result.adapter_response,
            'failures': run_result.failures,
            'execution_time': run_result.execution_time,
            'timing': run_result.timing,
            'node': run_result.node.to_dict() if run_result.node else None,
            'thread_id': run_result.thread_id
        }) %}
    {% endif %}
{% endmacro %}
