{% macro daily_buckets_cte() %}
    {{ adapter.dispatch('daily_buckets_cte','elementary')() }}
{% endmacro %}


{% macro default__daily_buckets_cte() -%}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (run_started_at - modules.datetime.timedelta(var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- set daily_buckets_cte %}
        with dates as (
            select {{ elementary.cast_as_timestamp(min_bucket_end) }} as date
        union all
        select {{ dbt_utils.dateadd('day', '1', 'date') }}
        from dates
        where {{ dbt_utils.dateadd('day', '1', 'date') }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
            )
        select date as edr_daily_bucket
        from dates
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}


{% macro bigquery__daily_buckets_cte() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (run_started_at - modules.datetime.timedelta(var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- set daily_buckets_cte %}
        select edr_daily_bucket
        from unnest(generate_timestamp_array({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 day)) as edr_daily_bucket
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}