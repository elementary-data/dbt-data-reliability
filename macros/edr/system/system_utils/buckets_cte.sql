{% macro complete_buckets_cte(time_bucket) %}
    {%- set edr_bucket_end_expr = elementary.edr_timeadd(time_bucket.period, time_bucket.count, 'edr_bucket_start') %}
    {%- set edr_min_bucket_start_expr = elementary.edr_date_trunc(time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start()))) %}
    {%- set edr_max_bucket_end_expr = elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) %}
    {{ adapter.dispatch('complete_buckets_cte','elementary')(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) }}
{% endmacro %}

{% macro spark__complete_buckets_cte(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from (select explode(sequence({{ edr_min_bucket_start_expr }}, {{ edr_max_bucket_end_expr }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start)
        where {{ edr_bucket_end_expr }} <= {{ edr_max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro snowflake__complete_buckets_cte(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) -%}
    {%- set complete_buckets_cte %}
        with timestamps as (
          select {{ edr_min_bucket_start_expr }} as edr_bucket_start
          union all
          select {{ edr_bucket_end_expr }} as next_bucket
          from timestamps
          where next_bucket < {{ edr_max_bucket_end_expr }}
        )
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from timestamps
        where {{ edr_bucket_end_expr }} <= {{ edr_max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro bigquery__complete_buckets_cte(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) %}

    {%- if time_bucket.period | lower in ['second', 'minute', 'hour', 'day'] %}
        {%- set complete_buckets_cte %}
            select
              edr_bucket_start,
              {{ edr_bucket_end_expr }} as edr_bucket_end
            from unnest(generate_timestamp_array({{ edr_min_bucket_start_expr }}, {{ edr_max_bucket_end_expr }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start
            where {{ edr_bucket_end_expr }} <= {{ edr_max_bucket_end_expr }}
        {%- endset %}
    {%- elif time_bucket.period | lower in ['week', 'month', 'quarter', 'year'] %}
        {%- set complete_buckets_cte %}
            select
              {{ elementary.edr_cast_as_timestamp('edr_bucket_start') }} as edr_bucket_start,
              {{ elementary.edr_cast_as_timestamp(edr_bucket_end_expr) }} as edr_bucket_end
            from unnest(generate_date_array({{ elementary.edr_cast_as_date(edr_min_bucket_start_expr) }}, {{ elementary.edr_cast_as_date(edr_max_bucket_end_expr) }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start
            where {{ elementary.edr_cast_as_timestamp(edr_bucket_end_expr) }} <= {{ edr_max_bucket_end_expr }}
        {%- endset %}
    {%- else %}
        {{ exceptions.raise_compiler_error("Unsupported time bucket period: ".format(time_bucket.period)) }}
    {%- endif %}

    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro redshift__complete_buckets_cte(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) %}
    {%- set complete_buckets_cte %}
      with integers as (
        select (row_number() over (order by 1)) - 1 as num
        from pg_catalog.pg_class
        limit {{ elementary.edr_datediff(edr_min_bucket_start_expr, edr_max_bucket_end_expr, time_bucket.period) }} / {{ time_bucket.count }} + 1
      )
      select
        {{ edr_min_bucket_start_expr }} + (num * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_start,
        {{ edr_min_bucket_start_expr }} + ((num + 1) * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_end
      from integers
      where edr_bucket_end <= {{ edr_max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro postgres__complete_buckets_cte(time_bucket, edr_bucket_end_expr, edr_min_bucket_start_expr, edr_max_bucket_end_expr) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from generate_series({{ edr_min_bucket_start_expr }}, {{ edr_max_bucket_end_expr }}, interval '{{ time_bucket.count }} {{ time_bucket.period }}') edr_bucket_start
        where {{ edr_bucket_end_expr }} <= {{ edr_max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}
