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


{% macro min_start_time(days_back, max_timeframe_end) %}

    {%- set days_subtract = '-' ~ days_back %}

    {%- set query_start_time %}
        with start_times as (
            select monitors_run_end as last_run,
                   cast({{ dbt_utils.dateadd('day', days_subtract, max_timeframe_end ) }} as {{ dbt_utils.type_timestamp() }}) as start_limit
            from {{ ref('elementary_runs')}}
                where monitors_run_end is not null
        order by monitors_run_end desc
        limit 1
        )
        select
            case
                when cast(start_limit as {{ dbt_utils.type_timestamp() }}) > cast(last_run as {{ dbt_utils.type_timestamp() }}) then {{ elementary.date_trunc('day', 'start_limit') }}
                else {{ elementary.date_trunc('day', 'last_run') }} end as start_time
        from start_times
    {%- endset -%}

    {%- set result_value = elementary.result_value(query_start_time) %}
    {%- if result_value is defined and result_value is not none %}
        {%- set min_start_time = result_value.strftime("%Y-%m-%d 00:00:00") %}
    {%- else %}
        {%- set min_start_time = (run_started_at - modules.datetime.timedelta(days_back)).strftime("%Y-%m-%d 00:00:00") %}
    {%- endif %}

    {{ return(min_start_time) }}

{% endmacro %}