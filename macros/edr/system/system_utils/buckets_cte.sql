{% macro complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) %}
    {%- set time_bucket = metric_properties.time_bucket %}
    {%- set bucket_end_expr = elementary.edr_timeadd(
        time_bucket.period, time_bucket.count, "edr_bucket_start"
    ) %}
    {%- set min_bucket_start_expr = elementary.edr_cast_as_timestamp(
        min_bucket_start
    ) %}
    {%- set max_bucket_end_expr = elementary.edr_cast_as_timestamp(max_bucket_end) %}
    {{
        adapter.dispatch("complete_buckets_cte", "elementary")(
            time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
        )
    }}
{% endmacro %}

{% macro default__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {{
        exceptions.raise_compiler_error(
            "The adapter does not have an implementation for macro 'complete_buckets_cte'"
        )
    }}
    {{ return("") }}
{% endmacro %}

{% macro clickhouse__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    with
        numbers as (
            select
                arrayJoin(
                    range(
                        0,
                        toUInt32(
                            {{
                                elementary.edr_datediff(
                                    min_bucket_start_expr,
                                    max_bucket_end_expr,
                                    time_bucket.period,
                                )
                            }} / {{ time_bucket.count }}
                        )
                        + 1
                    )
                ) as n
        )
    select
        {{
            elementary.edr_timeadd(
                time_bucket.period, "n * " ~ time_bucket.count, min_bucket_start_expr
            )
        }} as edr_bucket_start,
        {{
            elementary.edr_timeadd(
                time_bucket.period,
                "(n + 1) * " ~ time_bucket.count,
                min_bucket_start_expr,
            )
        }} as edr_bucket_end
    from numbers
    where
        {{
            elementary.edr_timeadd(
                time_bucket.period,
                "(n + 1) * " ~ time_bucket.count,
                min_bucket_start_expr,
            )
        }} <= {{ max_bucket_end_expr }}
{% endmacro %}

{% macro spark__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from (select explode(sequence({{ min_bucket_start_expr }}, {{ max_bucket_end_expr }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start)
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro fabricspark__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {{ return(elementary.spark__complete_buckets_cte(
        time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
    )) }}
{% endmacro %}

{% macro snowflake__complete_buckets_cte(
    time_bucket,
    bucket_end_expr,
    min_bucket_start_expr,
    max_bucket_end_expr
) -%}
    {%- set complete_buckets_cte %}
        with timestamps as (
          select {{ min_bucket_start_expr }} as edr_bucket_start
          union all
          select {{ bucket_end_expr }} as next_bucket
          from timestamps
          where next_bucket < {{ max_bucket_end_expr }}
        )
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from timestamps
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro bigquery__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}

    {%- if time_bucket.period | lower in ["second", "minute", "hour", "day"] %}
        {%- set complete_buckets_cte %}
            select
              edr_bucket_start,
              {{ bucket_end_expr }} as edr_bucket_end
            from unnest(generate_timestamp_array({{ min_bucket_start_expr }}, {{ max_bucket_end_expr }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start
            where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
        {%- endset %}
    {%- elif time_bucket.period | lower in ["week", "month", "quarter", "year"] %}
        {%- set complete_buckets_cte %}
            select
              {{ elementary.edr_cast_as_timestamp('edr_bucket_start') }} as edr_bucket_start,
              {{ elementary.edr_cast_as_timestamp(bucket_end_expr) }} as edr_bucket_end
            from unnest(generate_date_array({{ elementary.edr_cast_as_date(min_bucket_start_expr) }}, {{ elementary.edr_cast_as_date(max_bucket_end_expr) }}, interval {{ time_bucket.count }} {{ time_bucket.period }})) as edr_bucket_start
            where {{ elementary.edr_cast_as_timestamp(bucket_end_expr) }} <= {{ max_bucket_end_expr }}
        {%- endset %}
    {%- else %}
        {{
            exceptions.raise_compiler_error(
                "Unsupported time bucket period: ".format(time_bucket.period)
            )
        }}
    {%- endif %}

    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro redshift__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
      with integers as (
        select (row_number() over (order by 1)) - 1 as num
        from pg_catalog.pg_class
        limit {{ elementary.edr_datediff(min_bucket_start_expr, max_bucket_end_expr, time_bucket.period) }} / {{ time_bucket.count }} + 1
      )
      select
        {{ min_bucket_start_expr }} + (num * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_start,
        {{ min_bucket_start_expr }} + ((num + 1) * interval '{{ time_bucket.count }} {{ time_bucket.period }}') as edr_bucket_end
      from integers
      where edr_bucket_end <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}


{% macro postgres__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from generate_series({{ min_bucket_start_expr }}, {{ max_bucket_end_expr }}, interval '{{ time_bucket.count }} {{ time_bucket.period }}') edr_bucket_start
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro athena__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from unnest(sequence(
          {{ min_bucket_start_expr }},
          {{ max_bucket_end_expr }},
          {%- if time_bucket.period | lower == 'week' %}
            interval '{{ time_bucket.count * 7 }}' day
          {%- else %}
            interval '{{ time_bucket.count }}' {{ time_bucket.period }}
          {%- endif %}
        )) as t(edr_bucket_start)
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro trino__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from unnest(sequence(
          {{ min_bucket_start_expr }},
          {{ max_bucket_end_expr }},
          {%- if time_bucket.period | lower == 'week' %}
            interval '{{ time_bucket.count * 7 }}' day
          {%- else %}
            interval '{{ time_bucket.count }}' {{ time_bucket.period }}
          {%- endif %}
        )) as t(edr_bucket_start)
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro duckdb__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        select
          unnest(generate_series({{ min_bucket_start_expr }}, {{ max_bucket_end_expr }}, interval '{{ time_bucket.count }} {{ time_bucket.period }}')) as edr_bucket_start
    {%- endset %}
    {%- set complete_buckets_cte %}
        select
          edr_bucket_start,
          {{ bucket_end_expr }} as edr_bucket_end
        from ({{ complete_buckets_cte }}) as _buckets
        where {{ bucket_end_expr }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro fabric__complete_buckets_cte(
    time_bucket,
    bucket_end_expr,
    min_bucket_start_expr,
    max_bucket_end_expr
) -%}
    {# Fabric / T-SQL: use inline tally via VALUES cross-join instead of a recursive CTE.
       This avoids a WITH clause so the result can safely be embedded in a subquery
       or inside another CTE without triggering the T-SQL nested-CTE restriction.
       Supports up to 10 000 buckets (10^4). #}
    {%- set complete_buckets_cte %}
        select
            {{ elementary.edr_timeadd(time_bucket.period, "num * " ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_start,
            {{ elementary.edr_timeadd(time_bucket.period, "(num + 1) * " ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_end
        from (
            select (row_number() over (order by (select null))) - 1 as num
            from (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t1(val)
            cross join (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t2(val)
            cross join (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t3(val)
            cross join (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t4(val)
        ) as integers
        where {{ elementary.edr_timeadd(time_bucket.period, "(num + 1) * " ~ time_bucket.count, min_bucket_start_expr) }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro vertica__complete_buckets_cte(
    time_bucket,
    bucket_end_expr,
    min_bucket_start_expr,
    max_bucket_end_expr
) -%}
    {%- set complete_buckets_cte %}
        with integers as (
            select (row_number() over (order by t1.v, t2.v, t3.v, t4.v)) - 1 as num
            from (select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9 union all select 10) t1(v)
            cross join (select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9 union all select 10) t2(v)
            cross join (select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9 union all select 10) t3(v)
            cross join (select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9 union all select 10) t4(v)
        )
        select
            {{ elementary.edr_timeadd(time_bucket.period, 'num * ' ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_start,
            {{ elementary.edr_timeadd(time_bucket.period, '(num + 1) * ' ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_end
        from integers
        where {{ elementary.edr_timeadd(time_bucket.period, '(num + 1) * ' ~ time_bucket.count, min_bucket_start_expr) }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}

{% macro dremio__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {%- set complete_buckets_cte %}
        with integers as (
            select (row_number() over (order by t1.val, t2.val, t3.val, t4.val)) - 1 as num
            from (values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) t1(val)
            cross join (values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) t2(val)
            cross join (values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) t3(val)
            cross join (values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) t4(val)
        )
        select
            {{ elementary.edr_timeadd(time_bucket.period, 'num * ' ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_start,
            {{ elementary.edr_timeadd(time_bucket.period, '(num + 1) * ' ~ time_bucket.count, min_bucket_start_expr) }} as edr_bucket_end
        from integers
        where {{ elementary.edr_timeadd(time_bucket.period, '(num + 1) * ' ~ time_bucket.count, min_bucket_start_expr) }} <= {{ max_bucket_end_expr }}
    {%- endset %}
    {{ return(complete_buckets_cte) }}
{% endmacro %}
