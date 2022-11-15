{% macro edr_log(msg) %}
    {%- if execute %}
        {% do log('Elementary: ' ~ msg, info=True) %}
    {%- endif %}
{% endmacro %}

{% macro debug_log(msg) %}
    {%- if execute %}
        {% set debug_logs_enabled = elementary.get_config_var('debug_logs') %}
        {% if debug_logs_enabled %}
            {{ elementary.edr_log(msg) }}
        {% endif %}
    {%- endif %}
{% endmacro %}


{% macro test_log(msg_type, table_name, column_name=none) %}
    {%- if column_name %}
        {%- set start = 'Started running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
        {%- set end = 'Finished running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
    {%- else %}
        {%- set start = 'Started running data monitors on: ' ~ table_name %}
        {%- set end = 'Finished running data monitors on: ' ~ table_name %}
    {%- endif %}

    {%- if msg_type == 'start' %}
        {% do elementary.edr_log(start) %}
    {%- elif msg_type == 'end' %}
        {% do elementary.edr_log(end) %}
    {%- endif %}
{% endmacro %}
