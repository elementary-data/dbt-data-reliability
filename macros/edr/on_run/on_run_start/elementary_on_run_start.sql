{% macro elementary_on_run_start() %}
    {{ elementary.insert_invocation_start() }}
    {{ elementary.create_elementary_tests_schema() if flags.WHICH in ['test', 'build'] }}
{% endmacro %}

{% macro insert_invocation_start() %}
    {%- set invocation_times_table = elementary.get_invocations_table() %}
    {%- if invocation_times_table %}
        {%- set insert_invocation_query %}
            insert into {{ invocation_times_table }} values
            ('{{ invocation_id }}', {{ elementary.run_start_column() }}, {{ elementary.null_timestamp() }})
        {%- endset %}
        {% do run_query(insert_invocation_query) %}
    {%- endif %}
    {{ return('') }}
{% endmacro %}