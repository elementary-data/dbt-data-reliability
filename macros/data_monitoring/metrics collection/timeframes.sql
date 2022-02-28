{% macro max_timeframe_end(timeframe_duration) %}

    {%- set run_start_hour = run_started_at.strftime("%H") | int %}

    {%- if timeframe_duration == 24 %}
        {%- set max_timeframe_end = run_started_at.strftime("%Y-%m-%d 00:00:00") %}
    {%- elif timeframe_duration == 12 %}
        {%- if run_start_hour > 12 %}
            {%- set max_timeframe_end = run_started_at.strftime("%Y-%m-%d 12:00:00") %}
        {%- else %}
            {%- set max_timeframe_end = run_started_at.strftime("%Y-%m-%d 00:00:00") %}
        {%- endif %}
    {%- endif %}

    {{ return(max_timeframe_end) }}

{% endmacro %}


{% macro timeframe_to_query(days_back) %}

    {%- set timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d %H:%M:%S") ~ "'" %}
    {%- set days_subtract = '-' ~ days_back %}

    {%- set query_start_time %}
        with start_times as (
            select run_started_at as last_run, {{ dbt_utils.dateadd('day', days_subtract, timeframe_end ) }} as start_limit
            from {{ ref('elementary_runs')}}
            order by run_started_at desc
            limit 1 offset 1
        ),
        start_time_limit as (
            select case when start_limit > last_run then start_limit
               else last_run end as start_time
            from start_times
        )
        select {{ dbt_utils.datediff('start_time', timeframe_end, 'hour') }} as timeframe_to_query
        from start_time_limit
    {%- endset -%}

    {%- set result_value = elementary.result_value(query_start_time) %}
    {%- if result_value %}
        {%- set max_timeframe_to_query = result_value %}
    {%- else %}
        {%- set max_timeframe_to_query = 0 %}
    {%- endif %}
    {%- if max_timeframe_to_query < var('default_run_hours_back') %}
        {%- set timeframe_to_query = var('default_run_hours_back') %}
    {% else %}
        {%- set timeframe_to_query = max_timeframe_to_query %}
    {%- endif %}

    {{ return(timeframe_to_query) }}

{% endmacro %}