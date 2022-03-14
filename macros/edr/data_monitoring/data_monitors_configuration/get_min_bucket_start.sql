{% macro get_min_bucket_start(full_table_name,monitors=none,column_name=none) %}

    {%- set global_min_bucket_start = "'"~ (run_started_at - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with both_min_times as (
            select min(last_bucket_end) as min_bucket_start, {{ elementary.cast_as_timestamp(global_min_bucket_start) }} as global_min_start
            from {{ ref('monitors_runs') }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            )
        select case when min_bucket_start > global_min_start then min_bucket_start
            else global_min_start end as min_start
        from both_min_times
    {%- endset %}

    {%- set min_bucket_start_query_result = elementary.result_value(min_bucket_start_query) %}

    {%- if min_bucket_start_query_result %}
        {{ return(min_bucket_start_query_result) }}
    {%- else %}
        {{ return(global_min_bucket_start) }}
    {%- endif %}

{% endmacro %}