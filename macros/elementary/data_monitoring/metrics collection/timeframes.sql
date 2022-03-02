{% macro max_timeframe_end(timeframe_duration) %}

    {% set run_time = run_started_at %}
    {%- if timeframe_duration == 24 %}
        {%- set max_timeframe_end = run_time.strftime("%Y-%m-%d 00:00:00") %}
    {%- elif timeframe_duration == 12 %}
        {%- if run_time.hour > 12 %}
            {%- set max_timeframe_end = run_time.strftime("%Y-%m-%d 12:00:00") %}
        {%- else %}
            {%- set max_timeframe_end = run_time.strftime("%Y-%m-%d 00:00:00") %}
        {%- endif %}
    {%- endif %}

    {{ return(max_timeframe_end) }}

{% endmacro %}


{% macro hours_since_last_run(days_back, max_timeframe_end) %}

    {%- set days_subtract = '-' ~ days_back %}

    {%- set query_start_time %}
        with start_times as (
            select run_started_at as last_run, {{ dbt_utils.dateadd('day', days_subtract, max_timeframe_end ) }} as start_limit
            from {{ ref('elementary_runs')}}
            order by run_started_at desc
            limit 1 offset 1
        ),
        start_time_limit as (
            select case when start_limit > last_run then start_limit
               else last_run end as start_time
            from start_times
        )
        select {{ dbt_utils.datediff('start_time', max_timeframe_end, 'hour') }} as timeframe_to_query
        from start_time_limit
    {%- endset -%}

    {%- set result_value = elementary.result_value(query_start_time) %}
    {%- if result_value is defined and result_value is not none %}
        {%- set max_timeframe_to_query = result_value %}
    {%- else %}
        {%- set max_timeframe_to_query = days_back * 24 %}
    {%- endif %}

    {{ return(max_timeframe_to_query) }}

{% endmacro %}