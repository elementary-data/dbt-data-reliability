{% macro get_global_min_bucket_start() %}
    {%- set global_min_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {{ return(global_min_bucket_start) }}
{% endmacro %}

{% macro get_global_min_bucket_start_as_datetime() %}
    {%- set global_min_bucket_start = elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back')) %}
    {{ return(global_min_bucket_start) }}
{% endmacro %}

{# bucket_end represents the end of the bucket, so we need to add extra day to the timedelta #}
{% macro get_global_min_bucket_end_as_datetime() %}
    {%- set global_min_bucket_end = elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back') + 1) %}
    {{ return(global_min_bucket_end) }}
{% endmacro %}

{% macro get_max_bucket_end() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {{ return(max_bucket_end) }}
{% endmacro %}

{% macro get_backfill_bucket_start(backfill_days) %}
    {%- set backfill_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {{ return(backfill_bucket_start) }}
{% endmacro %}


{% macro get_min_bucket_start(full_table_name, backfill_days, monitors=none, column_name=none) %}

    {%- set global_min_bucket_start = elementary.get_global_min_bucket_start() %}
    {%- set backfill_bucket_start = elementary.get_backfill_bucket_start(backfill_days) %}

    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with min_times as (
            select min(last_bucket_end) as last_run,
                {{ elementary.cast_as_timestamp(global_min_bucket_start) }} as global_min_start,
                {{ elementary.cast_as_timestamp(backfill_bucket_start) }} as backfill_start
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