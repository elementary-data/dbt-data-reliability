{% macro period_buckets_cte(period) %}
    {%- set max_bucket_end = elementary.cast_as_timestamp(elementary.get_max_bucket_end(period)) %}
    {%- set min_bucket_end = elementary.cast_as_timestamp(elementary.get_min_bucket_end(period)) %}
    {{ adapter.dispatch('period_buckets_cte','elementary')(period, max_bucket_end, min_bucket_end) }}
{% endmacro %}


{% macro default__period_buckets_cte(period, max_bucket_end, min_bucket_end) -%}
    {%- set period_buckets_cte %}
        with time_frames as (
            select {{ min_bucket_end }} as time_frame
        union all
        select {{ elementary.timeadd(period, '1', 'time_frame') }}
        from time_frames
        where {{ elementary.timeadd(period, '1', 'time_frame') }} <= {{ max_bucket_end }}
            )
        select time_frame as edr_period_bucket
        from time_frames
        order by edr_period_bucket desc
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}


{% macro bigquery__period_buckets_cte(period, max_bucket_end, min_bucket_end) %}
    {%- set period_buckets_cte %}
        select edr_period_bucket
        from unnest(generate_timestamp_array({{ min_bucket_end }}, {{ max_bucket_end }}, interval 1 {{ period }})) as edr_period_bucket
        order by edr_period_bucket desc
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}


{% macro redshift__period_buckets_cte(period, max_bucket_end, min_bucket_end) %}
    {%- set period_buckets_cte %}
        with recursive time_frames(time_frame) as
           (
            select {{ min_bucket_end }} as time_frame
                union all
            select dateadd({{ period }}, 1, cast(time_frame as TIMESTAMP))
            from time_frames tf
            where tf.time_frame < {{ max_bucket_end }}
           )
        select time_frame as edr_period_bucket from time_frames
        order by edr_period_bucket desc
    {%- endset %}
    {{ return(period_buckets_cte) }}
{% endmacro %}


{% macro databricks__period_buckets_cte(period, max_bucket_end, min_bucket_end) %}
    select edr_period_bucket
    from (select explode(sequence({{ elementary.cast_as_timestamp(min_bucket_end) }}, {{ elementary.cast_as_timestamp(max_bucket_end) }}, interval 1 {{ period }})) AS edr_period_bucket)
    order by edr_period_bucket desc
{% endmacro %}
