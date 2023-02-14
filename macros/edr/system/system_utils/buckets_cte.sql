{% macro complete_buckets_cte(time_bucket) %}
    {{ adapter.dispatch('complete_buckets_cte','elementary')(time_bucket) }}
{% endmacro %}

{% macro spark__complete_buckets_cte(time_bucket) %}
    {% set edr_bucket_end_expr = elementary.edr_timeadd(time_bucket.period, time_bucket.count, 'edr_bucket_start') %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from (select explode(sequence({{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }}, {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start)
        where {{ edr_bucket_end_expr }} <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro snowflake__complete_buckets_cte(time_bucket) -%}
    {% set edr_bucket_end_expr = elementary.edr_timeadd(time_bucket.period, time_bucket.count, 'edr_bucket_start') %}
    {%- set complete_buckets_cte %}
        with timestamps as (
          select {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }} as edr_bucket_start
          union all
          select {{ edr_bucket_end_expr }} as next_bucket
          from timestamps
          where next_bucket < {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
        )
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from timestamps
        where {{ edr_bucket_end_expr }} <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro bigquery__complete_buckets_cte(time_bucket) %}
    {% set edr_bucket_end_expr = elementary.edr_timeadd(time_bucket.period, time_bucket.count, 'edr_bucket_start') %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from unnest(generate_timestamp_array({{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }}, {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start
        where {{ edr_bucket_end_expr }} <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro redshift__complete_buckets_cte(time_bucket) %}
    {%- set complete_buckets_cte %}
      with integers as (
        select (row_number() over (order by 1)) - 1 as num
        from pg_catalog.pg_class
        limit {{ elementary.timediff(time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())), elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end()))) }} / {{ time_bucket.count }} + 1
      )
      select
        {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }} + (num * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_start,
        {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }} + ((num + 1) * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_end
      from integers
      where edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro postgres__complete_buckets_cte(time_bucket) %}
    {% set edr_bucket_end_expr = elementary.edr_timeadd(time_bucket.period, time_bucket.count, 'edr_bucket_start') %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ edr_bucket_end_expr }} as edr_bucket_end
        from generate_series({{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_min_bucket_start())) }}, {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}, interval '{{ time_bucket.count }} {{ time_bucket.period }}') edr_bucket_start
        where {{ edr_bucket_end_expr }} <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}
