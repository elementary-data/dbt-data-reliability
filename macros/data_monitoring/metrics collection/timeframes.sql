{% macro zscore_timeframe_start() %}
    {%- set max_timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set days_back = '-' ~ var('days_back') %}
    {%- set zscore_timeframe_start = dbt_utils.dateadd('day', days_back, max_timeframe_end) %}
    {{ return(zscore_timeframe_start) }}
{% endmacro %}


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


-- for each metric+table or metric+column, i need to know when was the last time it was collected
-- so I could support configuration changes and gaps in runs.
-- do you have an idea for that?
-- could you pass me a 'new' flag for metric added to the config? maybe I can decide on what is new with the validated_config?

-- idea: let's re-run on table if something was added to it's config (easier than setting timeframe for each metric)

{% macro timeframe_for_query(days_back) %}

    {%- set timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d %H:%M:%S") ~ "'" %}
    {%- set days_subtract = '-' ~ days_back %}

    {%- set query_start_time %}
        with start_times as (
            select run_started_at as last_run, {{ dbt_utils.dateadd('day', days_subtract, timeframe_end ) }} as start_limit
            from {{ ref('elementary_runs')}}
            order by run_started_at_timestamp desc
            limit 2 offset 1
        )
        select case when start_limit > last_run then start_limit
               else last_run end as start_time
        from start_times
    {%- endset -%}

    {%- set timeframe_start_limit = result_column_to_list(query_start_time)[0] %}

    {% if timeframe_start_limit|length %}
        {%- set timeframe_start = timeframe_start_limit %}
    {% else %}
    -- figure out this else
        {%- set timeframe_start = 0%}
    {% endif %}

{% endmacro %}