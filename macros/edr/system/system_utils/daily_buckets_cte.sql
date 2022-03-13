{% macro daily_buckets_cte() %}
    {{ adapter.dispatch('daily_buckets_cte','elementary')() }}
{% endmacro %}


{% macro default__daily_buckets_cte() -%}
    {%- set daily_buckets_cte %}
        with dates as (
            select {{ elementary.date_trunc('day', 'min(bucket_end)') }} as date
            from {{ ref('data_monitoring_metrics') }}
        union all
        select {{ dbt_utils.dateadd('day', '1', 'date') }}
        from dates
        where {{ dbt_utils.dateadd('day', '1', 'date') }} <= {{ elementary.cast_to_timestamp(timeframe_end) }}
            )
        select date as edr_daily_bucket
        from dates
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}


{% macro bigquery__daily_buckets_cte() %}
    {%- set min_query %}
        select min(bucket_end) as date
        from {{ ref('data_monitoring_metrics') }}
    {%- endset %}

    {%- set max_query %}
        select max(bucket_end) as date
        from {{ ref('data_monitoring_metrics') }}
    {%- endset %}

    {%- set max_bucket_end = elementary.result_value(max_query) %}
    {%- set min_bucket_end = elementary.result_value(min_query) %}

    {%- if not max_bucket_end %}
        {%- set max_bucket_end = run_started_at.strftime("%Y-%m-%d 00:00:00") %}
        {%- set min_bucket_end = run_started_at.strftime("%Y-%m-%d 00:00:00") %}
    {%- endif %}

    {%- set daily_buckets_cte %}
        select edr_daily_bucket
        from unnest(generate_timestamp_array('{{ min_bucket_end }}', '{{ max_bucket_end }}', interval 1 day)) as edr_daily_bucket
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}