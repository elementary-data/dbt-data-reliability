{% macro daily_buckets_cte() %}
    {{ adapter.dispatch('daily_buckets_cte','elementary')() }}
{% endmacro %}

{# Databricks and Spark #}
{% macro default__daily_buckets_cte() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    select edr_daily_bucket
    from (select explode(sequence({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 day)) AS edr_daily_bucket)
{% endmacro %}

{% macro snowflake__daily_buckets_cte() -%}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

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
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set min_bucket_end = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00") ~"'" %}

    {%- set daily_buckets_cte %}
        select edr_daily_bucket
        from unnest(generate_timestamp_array({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 day)) as edr_daily_bucket
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}

{% macro redshift__daily_buckets_cte() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00") ~"'" %}
    {%- set days_back = elementary.get_config_var('days_back') %}

    {%- set daily_buckets_cte %}
        {%- for i in range(0, days_back+1) %}
            {%- set daily_bucket = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(i)).strftime("%Y-%m-%d 00:00:00") ~"'" %}
            select {{ elementary.cast_as_timestamp(daily_bucket) }} as edr_daily_bucket
            {%- if not loop.last %} union all {%- endif %}
        {%- endfor %}
    {%- endset %}
    {{ return(daily_buckets_cte) }}
{% endmacro %}