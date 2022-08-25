{% macro period_buckets_cte(period) %}
    {{ adapter.dispatch('period_buckets_cte','elementary')(period) }}
{% endmacro %}


{% macro default__period_buckets_cte(period) -%}
    {%- set max_bucket_end = get_max_bucket_end(period) %}
    {%- set min_bucket_end = get_min_bucket_end(period) %}
    {%- set period_buckets_cte %}
        with dates as (
            select {{ elementary.cast_as_timestamp(min_bucket_end) }} as date
        union all
        select {{ elementary.timeadd(period, '1', 'date') }}
        from dates
        where {{ elementary.timeadd(period, '1', 'date') }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
            )
        select date as edr_period_bucket
        from dates
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}


{% macro bigquery__period_buckets_cte(period) %}
    {%- set max_bucket_end = get_max_bucket_end(period) %}
    {%- set min_bucket_end = get_min_bucket_end(period) %}

    {%- set period_buckets_cte %}
        select edr_period_bucket
        from unnest(generate_timestamp_array({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 {{ period }})) as edr_period_bucket
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}

{% macro redshift__period_buckets_cte(period) %}
    {# TODO redshift implementation of this CTE#}
    {%- set max_bucket_end = get_max_bucket_end(period) %}
    {%- set days_back = elementary.get_config_var('days_back') %}

    {%- set period_buckets_cte %}
        {%- for i in range(0, days_back+1) %}
            {%- set daily_bucket = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(i)).strftime("%Y-%m-%d 00:00:00") ~"'" %}
            select {{ elementary.cast_as_timestamp(daily_bucket) }} as edr_daily_bucket
            {%- if not loop.last %} union all {%- endif %}
        {%- endfor %}
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}

{% macro databricks__period_buckets_cte(period) %}
    {%- set max_bucket_end = get_max_bucket_end(period) %}
    {%- set min_bucket_end = get_min_bucket_end(period) %}

    select edr_period_bucket
    from (select explode(sequence({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 {{ period }})) AS edr_period_bucket)
{% endmacro %}
