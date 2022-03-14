{% macro edr_log(msg) %}
    {%- if execute %}
        {% do log('Elementary: ' ~ msg, info=True) %}
    {%- endif %}
{% endmacro %}

{% macro debug_log(msg) %}
    {%- if execute %}
        {% do log('EDR_LOG_PRE', info=True) %}
        {% do log(msg, info=True) %}
        {% do log('EDR_LOG_AFTER', info=True) %}
    {%- endif %}
{% endmacro %}


{% macro test_log(msg_type, table_name, column_name=none) %}
    {%- set start = 'Started running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
    {%- set end = 'Finished running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
    {%- set no_monitors = 'No data monitors to run on: ' ~ table_name ~ ' ' ~ column_name %}

    {%- if msg_type == 'start' %}
        {% do edr_log(start) %}
    {%- elif msg_type == 'end' %}
        {% do edr_log(end) %}
    {%- elif msg_type == 'no_monitors' %}
        {% do edr_log(no_monitors) %}
    {%- endif %}
{% endmacro %}
