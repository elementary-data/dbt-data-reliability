{% macro update_invocation_end() %}
    {%- set invocation_times_table = elementary.get_invocations_table() %}
    {%- if invocation_times_table %}
        {%- set update_invocation_end_query %}
            update {{ invocation_times_table }}
            set invocation_ended_at = {{ elementary.current_timestamp_column() }}
            where invocation_id = '{{ invocation_id }}'
        {%- endset %}
        {% do run_query(update_invocation_end_query) %}
    {%- endif %}
    {{ return('') }}
{% endmacro %}