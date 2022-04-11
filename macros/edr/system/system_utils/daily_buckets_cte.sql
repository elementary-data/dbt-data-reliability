{% macro daily_buckets_cte() %}
    {{ adapter.dispatch('daily_buckets_cte','elementary')() }}
{% endmacro %}


{% macro default__daily_buckets_cte() -%}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (run_started_at - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- set daily_buckets_cte %}
        with dates as (
            select {{ elementary.cast_as_timestamp(min_bucket_end) }} as date
        union all
        select {{ elementary.timeadd('day', '1', 'date') }}
        from dates
        where {{ elementary.timeadd('day', '1', 'date') }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
            )
        select date as edr_daily_bucket
        from dates
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}


{% macro bigquery__daily_buckets_cte() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (run_started_at - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- set daily_buckets_cte %}
        select edr_daily_bucket
        from unnest(generate_timestamp_array({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 day)) as edr_daily_bucket
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}

{% macro redshift__daily_buckets_cte() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (run_started_at - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set days_back = elementary.get_config_var('days_back') %}

    {%- set daily_buckets_cte %}
        select current_date::timestamp - (i * interval '1 day') as edr_daily_bucket
        from generate_series(0, {{ days_back }}) i
        where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_end) }}
        order by 1 desc
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}