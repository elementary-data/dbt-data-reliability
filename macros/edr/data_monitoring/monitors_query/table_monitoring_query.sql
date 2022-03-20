{% macro table_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, table_monitors, freshness_column=none) %}

    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (run_started_at - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set table_monitors_list = ['row_count'] %}

    with timeframe_data as (

        select *
        {% if is_timestamp -%}
             , {{ elementary.date_trunc('day', timestamp_column) }} as edr_bucket
        {%- else %}
            , {{ elementary.null_timestamp() }} as edr_bucket
        {%- endif %}
        from {{ elementary.from(full_table_name) }}
        where
        {% if is_timestamp -%}
            {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
            and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
        {%- else %}
            true
        {%- endif %}

    ),

    daily_buckets as (

        {{ elementary.daily_buckets_cte() }}
        where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_start) }} and edr_daily_bucket <= {{ elementary.cast_as_timestamp(max_bucket_start) }}
            and edr_daily_bucket >= (select min(edr_bucket) from timeframe_data)

    ),

    table_monitors as (

    {%- if 'row_count' in table_monitors %}
        select edr_daily_bucket, edr_bucket,
            {%- if 'row_count' in table_monitors %} case when edr_bucket is null then 0 else {{ elementary.row_count() }} end {%- else -%} {{ elementary.null_float() }} {% endif %} as row_count
        from daily_buckets left join timeframe_data on (edr_daily_bucket = edr_bucket)
        group by 1,2
            {%- else %}
                {{ elementary.empty_table([('edr_daily_bucket','timestamp'),('edr_bucket','timestamp'),('row_count','int'),('source_value','string')]) }}
            {%- endif %}

    ),

    table_monitors_unpivot as (

        {% for monitor in table_monitors_list %}
            select edr_daily_bucket as edr_bucket, '{{ monitor }}' as metric_name, {{ elementary.cast_as_float(monitor) }} as metric_value, {{ elementary.null_string() }} as source_value from table_monitors where {{ monitor }} is not null
            {% if not loop.last %} union all {% endif %}
        {%- endfor %}

    ),

    table_freshness as (

    {%- if 'freshness' in table_monitors and is_timestamp %}
        {%- if freshness_column is undefined or freshness_column is none %}
            {%- set freshness_column = timestamp_column %}
        {%- endif %}
        select
            edr_daily_bucket as edr_bucket,
            'freshness' as metric_name,
            {{ elementary.timediff('minute', elementary.cast_as_timestamp('max('~freshness_column~')'), elementary.cast_as_timestamp(dbt_utils.dateadd('day','1','edr_daily_bucket'))) }} as metric_value,
            {{ elementary.to_char('max('~freshness_column~')') }} as source_value
        from daily_buckets, {{ elementary.from(full_table_name) }}
        where {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(dbt_utils.dateadd('day','1','edr_daily_bucket')) }}
        group by 1,2
    {%- else %}
        {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('metric_value','int'),('source_value','string')]) }}
    {%- endif %}

    ),

    union_metrics as (

        select * from table_monitors_unpivot
        union all
        select * from table_freshness

    ),

    metrics_final as (

        select
            '{{ full_table_name }}' as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            source_value,
            {%- if timestamp_column %}
            edr_bucket as bucket_start,
            {{ elementary.cast_as_timestamp(dbt_utils.dateadd('day',1,'edr_bucket')) }} as bucket_end,
            24 as bucket_duration_hours
            {%- else %}
            {{ elementary.null_timestamp() }} as bucket_start,
            {{ elementary.null_timestamp() }} as bucket_end,
            {{ elementary.null_int() }} as bucket_duration_hours
            {%- endif %}
        from
            union_metrics
        where cast(metric_value as {{ dbt_utils.type_int() }}) < {{ elementary.get_config_var('max_int') }}

    )

    select
        {{ dbt_utils.surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'bucket_start',
            'bucket_end'
        ]) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{- dbt_utils.current_timestamp_in_utc() -}} as updated_at

    from metrics_final

{% endmacro %}