{% macro insert_invocation_start() %}
    {%- set invocation_times_relation = elementary.get_invocations_table() %}
    {%- if invocation_times_relation %}
        {%- set invocation_time_dict = [{'invocation_id': invocation_id, 'invocation_started_at': run_started_at.strftime("%Y-%m-%d %H:%M:%S") }] %}
        {{ elementary.insert_dicts(invocation_times_relation, invocation_time_dict) }}
    {%- endif %}
    {{ return('') }}
{% endmacro %}

