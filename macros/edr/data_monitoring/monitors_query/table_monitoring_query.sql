{% macro table_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, table_monitors, freshness_column=none) %}

    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (run_started_at - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set table_monitors_list = ['row_count'] %}

    {% if is_timestamp %}
        with timeframe_data as (
            select *,
                   {{ elementary.date_trunc('day', timestamp_column) }} as edr_bucket
            from {{ elementary.from(full_table_name) }}
            where
                {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
                and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
        ),

        daily_buckets as (
            {{ elementary.daily_buckets_cte() }}
            where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_start) }} and
                  edr_daily_bucket <= {{ elementary.cast_as_timestamp(max_bucket_start) }} and
                  edr_daily_bucket >= (select min(edr_bucket) from timeframe_data)
        ),

        row_count as (
            {%- if 'row_count' in table_monitors %}
                select edr_daily_bucket as edr_bucket,
                       'row_count' as metric_name,
                        {{ elementary.null_string() }} as source_value
                        case when edr_bucket is null then
                            0
                        else {{ elementary.cast_as_float(elementary.row_count()) }} end as metric_value
                from daily_buckets left join timeframe_data on (edr_daily_bucket = edr_bucket)
                group by 1,2,3
            {%- else %}
                {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('source_value','string'),('metric_value','int')]) }}
            {%- endif %}
        ),

        table_freshness as (
        {%- if 'freshness' in table_monitors %}
            {%- if freshness_column is undefined or freshness_column is none %}
                {%- set freshness_column = timestamp_column %}
            {%- endif %}
            select
                edr_daily_bucket as edr_bucket,
                'freshness' as metric_name,
                {{ elementary.to_char('max('~freshness_column~')') }} as source_value,
                {{ elementary.timediff('minute', elementary.cast_as_timestamp('max('~freshness_column~')'), elementary.timeadd('day','1','edr_daily_bucket')) }} as metric_value
            from daily_buckets, {{ elementary.from(full_table_name) }}
            where {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.timeadd('day','1','edr_daily_bucket') }}
            group by 1,2
        {%- else %}
            {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('source_value','string'),('metric_value','int')]) }}
        {%- endif %}
        ),
    {% else %}
        with row_count as (
            {%- if 'row_count' in table_monitors %}
                select
                    {{ elementary.cast_as_timestamp(max_bucket_start) }} as edr_bucket,
                    'row_count' as metric_name,
                    {{ elementary.null_string() }} as source_value,
                    {{ elementary.row_count() }} as metric_value
                from {{ elementary.from(full_table_name) }}
                group by 1,2,3
            {%- else %}
                {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('source_value','string'),('metric_value','int')]) }}
            {%- endif %}
        ),

        table_freshness as (
            {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('source_value','string'),('metric_value','int')]) }}
        ),
    {% endif %}

    union_metrics as (

        select * from row_count
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
            edr_bucket as bucket_start,
            {{ elementary.timeadd('day',1,'edr_bucket') }} as bucket_end,
            24 as bucket_duration_hours
        from
            union_metrics
        where (metric_value is not null and cast(metric_value as {{ dbt_utils.type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
              metric_value is null
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