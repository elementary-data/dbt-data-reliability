{% macro get_global_min_bucket_start(period) %}
    {%- set run_started_at = "'"~elementary.get_run_started_at()~"'" -%}
    {%- set global_min_bucket_start = timeadd(period, - elementary.get_config_var('days_back'), date_trunc(period, run_started_at)) %}
    {{ return(global_min_bucket_start) }}
{% endmacro %}

{% macro get_global_min_bucket_start_as_datetime() %}
    {%- set global_min_bucket_start = elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back')) %}
    {{ return(global_min_bucket_start) }}
{% endmacro %}

{# bucket_end represents the end of the bucket, so we need to add extra day to the timedelta #}
{% macro get_global_min_bucket_end_as_datetime(period) %}
    {# TODO find a more elegant solution #}
    {% if period == 'hour' %}
        {%- set global_min_bucket_end = elementary.get_run_started_at() - modules.datetime.timedelta(hours=elementary.get_config_var('days_back') + 1) %}
    {% elif period == 'day' %}
        {%- set global_min_bucket_end = elementary.get_run_started_at() - modules.datetime.timedelta(days=elementary.get_config_var('days_back') + 1) %}
    {% endif %}
    {{ return(global_min_bucket_end) }}
{% endmacro %}

{% macro get_max_bucket_start(period) %}
    {% if period == 'hour' %}
        {%- set max_bucket_end = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")~"'" %}
    {% elif period == 'day' %}
        {%- set max_bucket_end = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% endif %}
    {{ return(max_bucket_end) }}
{% endmacro %}

{% macro get_max_bucket_end(period) %}
    {% if period == 'hour' %}
        {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d %H:00:00")~"'" %}
    {% elif period == 'day' %}
        {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% endif %}
    {{ return(max_bucket_end) }}
{% endmacro %}

{% macro get_min_bucket_end(period) %}
    {% if period == 'hour' %}
        {%- set min_bucket_end = "'"~(elementary.get_run_started_at() - modules.datetime.timedelta(hours=elementary.get_config_var('days_back'))).strftime("%Y-%m-%d %H:00:00")~"'" %}
    {% elif period == 'day' %}
        {%- set min_bucket_end = "'"~(elementary.get_run_started_at() - modules.datetime.timedelta(days=elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% endif %}    
    {{ return(min_bucket_end) }}
{% endmacro %}

{% macro get_backfill_bucket_start(backfill_days, period) %}
    {%- set run_started_at = "'"~elementary.get_run_started_at()~"'" -%}
    {%- set backfill_bucket_start = timeadd(period, - backfill_days, date_trunc(period, run_started_at)) %}
    {{ return(backfill_bucket_start) }}
{% endmacro %}


{% macro get_min_bucket_start(full_table_name, backfill_days, period, monitors=none, column_name=none) %}

    {%- set global_min_bucket_start = elementary.get_global_min_bucket_start(period) %}
    {%- set backfill_bucket_start = elementary.get_backfill_bucket_start(backfill_days, period) %}
    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with min_times as (
            select min(last_bucket_end) as last_run,
                {{ global_min_bucket_start }} as global_min_start,
                {{ backfill_bucket_start }} as backfill_start
            from {{ ref('monitors_runs') }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            )
        select
            case
                when last_run is null then global_min_start
                when last_run < backfill_start then last_run
                else backfill_start
            end as min_start
        from min_times
    {%- endset %}
    {%- set min_bucket_start_query_result = elementary.result_value(min_bucket_start_query) %}

    {%- if min_bucket_start_query_result %}
        {{ return(min_bucket_start_query_result) }}
    {%- else %}
        {{ return(global_min_bucket_start) }}
    {%- endif %}

{% endmacro %}

{% macro get_metric_min_time(global_min_bucket_end, backfill_days, period) %}
    {# TODO find a more elegant solution #}
    {% if period == 'hour' %}
        {%- set truncated_global_min_bucket_end = modules.datetime.datetime(year=global_min_bucket_end.year, 
                                                                            month=global_min_bucket_end.month, 
                                                                            day=global_min_bucket_end.day,
                                                                            hour=global_min_bucket_end.hour,) %}
        {%- set metrics_min_time = truncated_global_min_bucket_end - modules.datetime.timedelta(hours=backfill_days + 1) %}
    {% elif period == 'day' %}
        {%- set truncated_global_min_bucket_end = modules.datetime.datetime(year=global_min_bucket_end.year, 
                                                                            month=global_min_bucket_end.month, 
                                                                            day=global_min_bucket_end.day,) %}
        {%- set metrics_min_time = truncated_global_min_bucket_end - modules.datetime.timedelta(days=backfill_days + 1) %}
    {% endif %}
    {{ return(metrics_min_time) }}
{% endmacro %}