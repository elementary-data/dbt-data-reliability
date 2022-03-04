{% macro zscore_timeframe_start() %}
    {%- set max_timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set days_back = '-' ~ var('days_back') %}
    {%- set zscore_timeframe_start = dbt_utils.dateadd('day', days_back, max_timeframe_end) %}
    {{ return(zscore_timeframe_start) }}
{% endmacro %}
