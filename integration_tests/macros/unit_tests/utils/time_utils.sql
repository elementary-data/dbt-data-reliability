{% macro time_diff(min_time, max_time, unit) %}
    {%- set start_time = modules.datetime.datetime.strptime(min_time, "%Y-%m-%dT%H:%M:%S") %}
    {%- set end_time = modules.datetime.datetime.strptime(max_time, "%Y-%m-%dT%H:%M:%S") %}
    {%- set delta = end_time - start_time %}

    {%- if unit == 'hours' %}
        {{ return(delta.total_seconds() / (60*60)) }}
    {%- elif unit == 'days' %}
        {{ return(delta.total_seconds() / (60*60*24)) }}
    {%- elif unit == 'weeks' %}
        {{ return(delta.total_seconds() / (60*60*24*7)) }}
    {%- elif unit == 'months' %}
        {%- set months = (end_time.year - start_time.year) * 12 + (end_time.month - start_time.month) %}
        {{ return(months) }}
    {%- endif %}
{% endmacro %}

{% macro assert_round_time(datetime, unit) %}
    {%- if unit == 'hours' %}
       {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%M:%S") %}
       {{ assert_value(time_string, '00:00') }}
    {%- elif unit == 'days' %}
       {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%H:%M:%S") %}
       {{ assert_value(time_string, '00:00:00') }}
    {%- elif unit == 'weeks' %}
        {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%w %H:%M:%S") %}
        {{ assert_value(time_string, '1 00:00:00') }}
    {%- elif unit == 'months' %}
        {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%d %H:%M:%S") %}
        {{ assert_value(time_string, '01 00:00:00') }}
    {%- endif %}
{% endmacro %}

{% macro get_x_time_ago(x, unit) %}
    {%- if unit == 'hours' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(hours=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- elif unit == 'days' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(days=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- elif unit == 'weeks' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(weeks=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- endif %}
    {{ return(x_time_ago) }}
{% endmacro %}